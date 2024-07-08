import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from 'typeorm';

@Entity('process_tracking')
@Index('idx_process_tracking_session_id_process_name_record_id', [
  'processName',
  'sessionId',
  'recordId',
])
@Index('idx_process_tracking_session_id_process_name', [
  'sessionId',
  'processName',
])
export class ProcessTracking {
  @PrimaryGeneratedColumn({ type: 'bigint' })
  id: number;

  @Column({ name: 'process_name', type: 'varchar', length: 50 })
  processName: string;

  @Column({ name: 'session_id', type: 'bigint' })
  sessionId: number;

  @Column({ name: 'record_id', type: 'int' })
  recordId: number;

  @CreateDateColumn({ name: 'created_at', type: 'timestamp' })
  createdAt: Date;
}
