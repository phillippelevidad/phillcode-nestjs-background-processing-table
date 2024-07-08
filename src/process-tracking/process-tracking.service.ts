import { Repository } from 'typeorm';
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { ProcessTracking } from './process-tracking.entity';

@Injectable()
export class ProcessTrackingService {
  constructor(
    @InjectRepository(ProcessTracking)
    private readonly processTrackingRepository: Repository<ProcessTracking>,
  ) {}

  async insertRecords(
    processName: string,
    sessionId: number,
    recordIds: number[],
  ): Promise<void> {
    const records = recordIds.map((recordId) => ({
      processName,
      sessionId,
      recordId,
    }));
    await this.processTrackingRepository.save(records);
  }

  async deleteRecords(processName: string, sessionId: number): Promise<void> {
    await this.processTrackingRepository.delete({
      processName,
      sessionId,
    });
  }
}
