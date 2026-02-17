package gatewayservices

import (
	"context"
	"io"
	"os"
	"path"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultObjectPutExpirationS = 60 * 60 * 24
)

func (gws *GatewayService) HeadObject(ctx context.Context, in *pb.HeadObjectRequest) (*pb.HeadObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	useWorkspaceStorage := authInfo.Workspace.StorageAvailable()
	existingObject, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil {
		exists := true

		// Check if the actual object file exists, not just the directory
		objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name, existingObject.ExternalId)
		if _, err := os.Stat(objectPath); os.IsNotExist(err) {
			exists = false
		}

		if useWorkspaceStorage {
			storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
			if err != nil {
				return &pb.HeadObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create storage client",
				}, nil
			}

			exists, err = storageClient.Exists(ctx, path.Join(types.DefaultObjectPrefix, existingObject.ExternalId))
			if err != nil {
				return &pb.HeadObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to check if object exists",
				}, nil
			}
		}

		if exists {
			return &pb.HeadObjectResponse{
				Ok:     true,
				Exists: true,
				ObjectMetadata: &pb.ObjectMetadata{
					Name: existingObject.Hash,
					Size: existingObject.Size,
				},
				ObjectId:            existingObject.ExternalId,
				UseWorkspaceStorage: useWorkspaceStorage,
			}, nil
		} else {
			return &pb.HeadObjectResponse{
				Ok:                  true,
				Exists:              false,
				UseWorkspaceStorage: useWorkspaceStorage,
			}, nil
		}
	}

	return &pb.HeadObjectResponse{
		Ok:                  true,
		Exists:              false,
		UseWorkspaceStorage: useWorkspaceStorage,
	}, nil
}

func (gws *GatewayService) CreateObject(ctx context.Context, in *pb.CreateObjectRequest) (*pb.CreateObjectResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
	os.MkdirAll(objectPath, 0644)

	storageClient, err := clients.NewWorkspaceStorageClient(ctx, authInfo.Workspace.Name, authInfo.Workspace.Storage)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to create storage client",
		}, nil
	}

	object, err := gws.backendRepo.GetObjectByHash(ctx, in.Hash, authInfo.Workspace.Id)
	if err == nil && !in.Overwrite {
		return &pb.CreateObjectResponse{
			Ok:       true,
			ObjectId: object.ExternalId,
		}, nil
	}

	if object == nil {
		object, err = gws.backendRepo.CreateObject(ctx, in.Hash, in.Size, authInfo.Workspace.Id)
		if err != nil {
			return &pb.CreateObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to create object",
			}, nil
		}
	}

	presignedURL, err := storageClient.GeneratePresignedPutURL(ctx, path.Join(types.DefaultObjectPrefix, object.ExternalId), defaultObjectPutExpirationS)
	if err != nil {
		return &pb.CreateObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to generate presigned URL",
		}, nil
	}

	return &pb.CreateObjectResponse{
		Ok:           true,
		ObjectId:     object.ExternalId,
		PresignedUrl: presignedURL,
	}, nil
}

func (gws *GatewayService) PutObjectStream(stream pb.GatewayService_PutObjectStreamServer) error {
	log.Info().Msg("PutObjectStream: starting")
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		log.Warn().Msg("PutObjectStream: unauthorized access")
		return status.Error(codes.PermissionDenied, "Unauthorized Access")
	}

	objectPath := path.Join(types.DefaultObjectPath, authInfo.Workspace.Name)
	os.MkdirAll(objectPath, 0644)

	var size int
	var file *os.File
	var newObject *types.Object
	var chunkCount int

	for {
		request, err := stream.Recv()
		if err == io.EOF {
			log.Info().Int("chunks", chunkCount).Int("total_size", size).Msg("PutObjectStream: EOF received")
			break
		}

		if err != nil {
			log.Error().Err(err).Msg("PutObjectStream: error receiving stream")
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to receive stream of bytes",
			})
		}

		chunkCount++
		if file == nil {
			log.Info().Str("hash", request.Hash).Msg("PutObjectStream: creating object")
			newObject, err = gws.backendRepo.CreateObject(ctx, request.Hash, 0, authInfo.Workspace.Id)
			if err != nil {
				log.Error().Err(err).Msg("PutObjectStream: error creating object in repo")
				return stream.SendAndClose(&pb.PutObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create object",
				})
			}

			filePath := path.Join(objectPath, newObject.ExternalId)
			log.Info().Str("path", filePath).Msg("PutObjectStream: creating file")
			file, err = os.Create(filePath)
			if err != nil {
				log.Error().Err(err).Str("path", filePath).Msg("PutObjectStream: error creating file")
				gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
				return stream.SendAndClose(&pb.PutObjectResponse{
					Ok:       false,
					ErrorMsg: "Unable to create file",
				})
			}
			defer file.Close()
		}

		s, err := file.Write(request.ObjectContent)
		if err != nil {
			log.Error().Err(err).Msg("PutObjectStream: error writing to file")
			os.Remove(path.Join(objectPath, newObject.ExternalId))
			gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to write file content",
			})
		}
		size += s
	}

	log.Info().Int("size", size).Msg("PutObjectStream: syncing file")
	// Sync file to ensure data is flushed to the filesystem (required for JuiceFS)
	if file != nil {
		if err := file.Sync(); err != nil {
			log.Error().Err(err).Msg("PutObjectStream: error syncing file")
			os.Remove(path.Join(objectPath, newObject.ExternalId))
			gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
			return stream.SendAndClose(&pb.PutObjectResponse{
				Ok:       false,
				ErrorMsg: "Unable to sync file content",
			})
		}
	}

	log.Info().Msg("PutObjectStream: updating object size")
	if err := gws.backendRepo.UpdateObjectSizeByExternalId(ctx, newObject.ExternalId, size); err != nil {
		log.Error().Err(err).Msg("PutObjectStream: error updating object size")
		os.Remove(path.Join(objectPath, newObject.ExternalId))
		gws.backendRepo.DeleteObjectByExternalId(ctx, newObject.ExternalId)
		return stream.SendAndClose(&pb.PutObjectResponse{
			Ok:       false,
			ErrorMsg: "Unable to complete file upload",
		})
	}

	log.Info().Str("object_id", newObject.ExternalId).Int("size", size).Msg("PutObjectStream: completed successfully")
	return stream.SendAndClose(&pb.PutObjectResponse{
		Ok:       true,
		ObjectId: newObject.ExternalId,
	})
}
