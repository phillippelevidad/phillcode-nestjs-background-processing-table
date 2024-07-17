import { Job, Queue } from 'bull';
import { ProcessTrackingService } from 'src/process-tracking/process-tracking.service';
import { DataSource } from 'typeorm';
import { InjectQueue, OnQueueFailed, Process, Processor } from '@nestjs/bull';
import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import { User } from './user.entity';

const QUEUE_NAME = 'cron-jobs';
const JOB_NAME = 'process-users';

const BATCH_SIZE = 1000;
const BATCH_DELAY = 1000;
const SLICE_SIZE = 100;

type UserRecord = {
  id: number;
  status: 'pending' | 'processed';
};

type JobData = {
  sessionId: number;
};

@Injectable()
@Processor(QUEUE_NAME)
export class ProcessUsersJob {
  private readonly logger = new Logger(ProcessUsersJob.name);

  constructor(
    private readonly dataSource: DataSource,
    private readonly processTracking: ProcessTrackingService,
    @InjectQueue(QUEUE_NAME)
    private readonly queue: Queue,
  ) {}

  @Cron(CronExpression.EVERY_DAY_AT_3AM)
  async run() {
    const sessionId = Date.now();
    await this.queue.add(JOB_NAME, { sessionId });
    this.logger.log(`Initial job added to the queue`);
  }

  @Process(JOB_NAME)
  protected async process(job: Job<JobData>) {
    this.logger.log(`Processing started`);

    const batch = await this.queryBatch(job.data.sessionId);
    await this.processBatch(batch, job.data.sessionId);

    if (batch.length === BATCH_SIZE) {
      await this.queue.add(
        JOB_NAME,
        { sessionId: job.data.sessionId },
        { delay: BATCH_DELAY },
      );
      this.logger.log(
        `More entries to process, scheduling next job with ${BATCH_DELAY}ms delay`,
      );
    } else {
      await this.processTracking.deleteRecords(JOB_NAME, job.data.sessionId);
      this.logger.log(`Processing completed`);
    }
  }

  private async queryBatch(sessionId: number) {
    this.logger.log(`Querying batch`);
    return this.dataSource.query<UserRecord[]>(
      `
      select id, status from users
      where status = 'pending' and id not in (select id from process_tracking where process_name = $1 and session_id = $2)
      limit ${BATCH_SIZE} for update skip locked`,
      [JOB_NAME, sessionId],
    );
  }

  private async processBatch(batch: UserRecord[], sessionId: number) {
    this.logger.log(`Processing batch of ${batch.length} records`);
    for (let i = 0; i < batch.length; i += SLICE_SIZE) {
      const slice = batch.slice(i, i + SLICE_SIZE);
      const results = await Promise.allSettled(
        slice.map((record) => this.processRecord(record)),
      );

      const errorIds: number[] = [];
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          errorIds.push(slice[index].id);
          this.logger.error(
            `Error processing record ${slice[index].id}: ${result.reason}`,
          );
        }
      });

      if (errorIds.length > 0) {
        await this.processTracking.insertRecords(JOB_NAME, sessionId, errorIds);
      }
    }
    this.logger.log(`Batch processed`);
  }

  private async processRecord(record: UserRecord) {
    this.logger.log(`Processing record ${record.id}`);
    await new Promise((resolve) =>
      setTimeout(resolve, Math.random() * (500 - 200) + 200),
    );
    if (Math.random() < 0.1) {
      throw new Error('Random error');
    }
    await this.dataSource.manager.update(User, record.id, {
      status: 'processed',
    });
  }

  @OnQueueFailed()
  protected async onQueueFailed(job: Job<JobData>, error: Error) {
    this.logger.error(`Job failed: ${error.message} ${error}`);
    if (job.attemptsMade === job.opts.attempts) {
      await this.processTracking.deleteRecords(JOB_NAME, job.data.sessionId);
    }
  }
}
