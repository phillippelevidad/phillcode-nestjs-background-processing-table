import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ProcessTracking } from './process-tracking.entity';
import { ProcessTrackingService } from './process-tracking.service';

@Module({
  imports: [TypeOrmModule.forFeature([ProcessTracking])],
  providers: [ProcessTrackingService],
  exports: [ProcessTrackingService],
})
export class ProcessTrackingModule {}
