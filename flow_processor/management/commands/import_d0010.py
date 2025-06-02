import os
import logging
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from flow_processor.models import FlowFile, MeterPoint, Meter, Reading
from flow_processor.parsers.d0010_parser import D0010Parser

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Import D0010 flow files into the database'
    
    def add_arguments(self, parser):
        parser.add_argument(
            'file_paths',
            nargs='+',
            type=str,
            help='Path(s) to D0010 file(s) to import'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Parse files without saving to database'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Re-import files even if already processed'
        )
    
    def handle(self, *args, **options):
        file_paths = options['file_paths']
        dry_run = options['dry_run']
        force = options['force']
        
        self.stdout.write(f"Processing {len(file_paths)} file(s)...")
        
        success_count = 0
        error_count = 0
        
        for file_path in file_paths:
            try:
                self._process_file(file_path, dry_run, force)
                success_count += 1
            except Exception as e:
                error_count += 1
                self.stdout.write(
                    self.style.ERROR(f"Failed to process {file_path}: {str(e)}")
                )
                logger.exception(f"Error processing {file_path}")
        
        # Summary
        self.stdout.write(
            self.style.SUCCESS(
                f"\nProcessing complete: {success_count} succeeded, {error_count} failed"
            )
        )
    
    def _process_file(self, file_path: str, dry_run: bool, force: bool):
        """Process a single D0010 file"""
        # Validate file exists
        if not os.path.exists(file_path):
            raise CommandError(f"File not found: {file_path}")
        
        if not os.path.isfile(file_path):
            raise CommandError(f"Not a file: {file_path}")
        
        filename = os.path.basename(file_path)
        self.stdout.write(f"\nProcessing {filename}...")
        
        # Parse the file
        parser = D0010Parser()
        readings_data, file_hash = parser.parse_file(file_path)
        
        if not readings_data:
            self.stdout.write(self.style.WARNING(f"No valid readings found in {filename}"))
            return
        
        # Check if file already imported
        if not force:
            existing = FlowFile.objects.filter(file_hash=file_hash).first()
            if existing:
                self.stdout.write(
                    self.style.WARNING(
                        f"File {filename} already imported on {existing.imported_at}. "
                        "Use --force to re-import."
                    )
                )
                return
        
        if dry_run:
            self.stdout.write(f"Dry run: Would import {len(readings_data)} readings")
            if parser.warnings:
                self.stdout.write(self.style.WARNING(f"Warnings: {len(parser.warnings)}"))
                for warning in parser.warnings[:10]:  # Show first 10 warnings
                    self.stdout.write(f"  - {warning}")
            return
        
        # Import data in a transaction
        with transaction.atomic():
            # Create or update flow file record
            flow_file, created = FlowFile.objects.update_or_create(
                file_hash=file_hash,
                defaults={
                    'filename': filename,
                    'status': 'processing',
                    'row_count': len(readings_data),
                }
            )
            
            try:
                imported_count = self._import_readings(flow_file, readings_data)
                
                # Update status
                flow_file.status = 'completed'
                flow_file.save()
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Successfully imported {imported_count} readings from {filename}"
                    )
                )
                
                if parser.warnings:
                    self.stdout.write(
                        self.style.WARNING(f"Completed with {len(parser.warnings)} warnings")
                    )
                
            except Exception as e:
                flow_file.status = 'failed'
                flow_file.error_message = str(e)
                flow_file.save()
                raise
    
    def _import_readings(self, flow_file: FlowFile, readings_data: list) -> int:
        """Import readings into database"""
        imported_count = 0
        
        for reading_data in readings_data:
            # Get or create meter point
            meter_point, _ = MeterPoint.objects.get_or_create(
                mpan=reading_data['mpan']
            )
            
            # Get or create meter
            meter, _ = Meter.objects.get_or_create(
                serial_number=reading_data['meter_serial'],
                defaults={'meter_point': meter_point}
            )
            
            # Update meter point if different
            if meter.meter_point != meter_point:
                logger.warning(
                    f"Meter {meter.serial_number} moved from "
                    f"MPAN {meter.meter_point.mpan} to {meter_point.mpan}"
                )
                meter.meter_point = meter_point
                meter.save()
            
            # Create or update reading
            reading, created = Reading.objects.update_or_create(
                meter=meter,
                reading_date=reading_data['reading_date'],
                reading_time=reading_data['reading_time'],
                register_id=reading_data['register_id'],
                defaults={
                    'flow_file': flow_file,
                    'reading_value': reading_data['reading_value'],
                    'reading_type': reading_data['reading_type'],
                }
            )
            
            if created:
                imported_count += 1
            else:
                logger.info(f"Updated existing reading for {meter.serial_number} on {reading_data['reading_date']}")
        
        return imported_count