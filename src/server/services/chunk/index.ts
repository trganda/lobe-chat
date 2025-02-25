import { fileEnv } from '@/config/file';
import { JWTPayload } from '@/const/auth';
import { serverDB } from '@/database/server';
import { AsyncTaskModel } from '@/database/server/models/asyncTask';
import { FileModel } from '@/database/server/models/file';
import { ChunkContentParams, ContentChunk } from '@/server/modules/ContentChunk';
import { createAsyncServerClient } from '@/server/routers/async';
import {
  AsyncTaskError,
  AsyncTaskErrorType,
  AsyncTaskStatus,
  AsyncTaskType,
} from '@/types/asyncTask';

export class ChunkService {
  private userId: string;
  private chunkClient: ContentChunk;
  private fileModel: FileModel;
  private asyncTaskModel: AsyncTaskModel;

  constructor(userId: string) {
    this.userId = userId;

    this.chunkClient = new ContentChunk();

    this.fileModel = new FileModel(serverDB, userId);
    this.asyncTaskModel = new AsyncTaskModel(serverDB, userId);
  }

  async chunkContent(params: ChunkContentParams) {
    return this.chunkClient.chunkContent(params);
  }

  async asyncEmbeddingFileChunks(fileId: string, payload: JWTPayload) {
    const result = await this.fileModel.findById(fileId);

    if (!result) return;

    // 1. create a asyncTaskId
    const asyncTaskId = await this.asyncTaskModel.create({
      status: AsyncTaskStatus.Pending,
      type: AsyncTaskType.Embedding,
    });

    await this.fileModel.update(fileId, { embeddingTaskId: asyncTaskId });

    const asyncCaller = await createAsyncServerClient(this.userId, payload);

    // trigger embedding task asynchronously
    try {
      await asyncCaller.file.embeddingChunks.mutate({ fileId, taskId: asyncTaskId });
    } catch (e) {
      console.error('[embeddingFileChunks] error:', e);

      await this.asyncTaskModel.update(asyncTaskId, {
        error: new AsyncTaskError(
          AsyncTaskErrorType.TaskTriggerError,
          'trigger chunk embedding async task error. Please make sure the APP_URL is available from your server. You can check the proxy config or WAF blocking',
        ),
        status: AsyncTaskStatus.Error,
      });
    }

    return asyncTaskId;
  }

  /**
   * parse file to chunks with async task
   */
  async asyncParseFileToChunks(fileId: string, payload: JWTPayload, skipExist?: boolean) {
    const result = await this.fileModel.findById(fileId);

    if (!result) return;

    // skip if already exist chunk tasks and the chunk task was successful
    if (skipExist && result.chunkTaskId) {
      const chunk_task_result = await this.asyncTaskModel.findById(result.chunkTaskId);

      if (chunk_task_result) {
        if (chunk_task_result.status === AsyncTaskStatus.Success) {
          // check if embedding task exists and is successful
          if (result.embeddingTaskId) {
            const embedding_task_result = await this.asyncTaskModel.findById(
              result.embeddingTaskId,
            );

            if (embedding_task_result && embedding_task_result.status === AsyncTaskStatus.Success) {
              return;
            } else {
              // trigger embedding if auto embedding is enabled
              if (fileEnv.CHUNKS_AUTO_EMBEDDING) {
                return await this.asyncEmbeddingFileChunks(fileId, payload);
              }
            }
          }
          return;
        }

        // handle unsupported file type
        if (
          chunk_task_result.status === AsyncTaskStatus.Error &&
          typeof chunk_task_result.error === 'string' &&
          chunk_task_result.error.includes('Unsupported file type')
        ) {
          return;
        }
      }
    }

    // 1. create a asyncTaskId
    const asyncTaskId = await this.asyncTaskModel.create({
      status: AsyncTaskStatus.Processing,
      type: AsyncTaskType.Chunking,
    });

    await this.fileModel.update(fileId, { chunkTaskId: asyncTaskId });

    const asyncCaller = await createAsyncServerClient(this.userId, payload);

    // trigger parse file task asynchronously
    asyncCaller.file.parseFileToChunks
      .mutate({ fileId: fileId, taskId: asyncTaskId })
      .catch(async (e) => {
        console.error('[ParseFileToChunks] error:', e);

        await this.asyncTaskModel.update(asyncTaskId, {
          error: new AsyncTaskError(
            AsyncTaskErrorType.TaskTriggerError,
            'trigger chunk embedding async task error. Please make sure the APP_URL is available from your server. You can check the proxy config or WAF blocking',
          ),
          status: AsyncTaskStatus.Error,
        });
      });

    return asyncTaskId;
  }
}
