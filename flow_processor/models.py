from django.db import models
from django.core.validators import RegexValidator
from django.utils import timezone


class FlowFile(models.Model):
    """Represents an imported D0010 flow file"""
    filename = models.CharField(max_length=255, unique=True)
    file_hash = models.CharField(max_length=64, unique=True)
    imported_at = models.DateTimeField(default=timezone.now)
    row_count = models.IntegerField(default=0)
    status = models.CharField(
        max_length=20,
        choices=[
            ('pending', 'Pending'),
            ('processing', 'Processing'),
            ('completed', 'Completed'),
            ('failed', 'Failed'),
        ],
        default='pending'
    )
    error_message = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ['-imported_at']
        indexes = [
            models.Index(fields=['filename']),
            models.Index(fields=['imported_at']),
        ]

    def __str__(self):
        return f"{self.filename} ({self.imported_at.strftime('%Y-%m-%d %H:%M')})"


class MeterPoint(models.Model):
    """Represents a meter point identified by MPAN"""
    mpan_validator = RegexValidator(
        regex=r'^\d{13}$',
        message='MPAN must be exactly 13 digits'
    )
    
    mpan = models.CharField(
        max_length=13,
        unique=True,
        validators=[mpan_validator],
        db_index=True
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['mpan']

    def __str__(self):
        return f"MPAN: {self.mpan}"


class Meter(models.Model):
    """Represents a physical meter device"""
    serial_number = models.CharField(max_length=50, unique=True, db_index=True)
    meter_point = models.ForeignKey(
        MeterPoint,
        on_delete=models.CASCADE,
        related_name='meters'
    )
    meter_type = models.CharField(
        max_length=20,
        choices=[
            ('single', 'Single Rate'),
            ('economy7', 'Economy 7'),
            ('smart', 'Smart Meter'),
        ],
        default='single'
    )
    installed_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['serial_number']
        indexes = [
            models.Index(fields=['serial_number']),
            models.Index(fields=['meter_point']),
        ]

    def __str__(self):
        return f"Meter: {self.serial_number} (MPAN: {self.meter_point.mpan})"


class Reading(models.Model):
    """Represents a meter reading"""
    meter = models.ForeignKey(
        Meter,
        on_delete=models.CASCADE,
        related_name='readings'
    )
    flow_file = models.ForeignKey(
        FlowFile,
        on_delete=models.CASCADE,
        related_name='readings'
    )
    reading_date = models.DateField(db_index=True)
    reading_time = models.TimeField(null=True, blank=True)
    register_id = models.CharField(max_length=10, default='01')
    reading_value = models.DecimalField(max_digits=10, decimal_places=2)
    reading_type = models.CharField(
        max_length=20,
        choices=[
            ('actual', 'Actual'),
            ('estimated', 'Estimated'),
            ('customer', 'Customer'),
        ],
        default='actual'
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-reading_date', '-reading_time']
        indexes = [
            models.Index(fields=['meter', 'reading_date']),
            models.Index(fields=['reading_date']),
        ]
        unique_together = [
            ['meter', 'reading_date', 'reading_time', 'register_id']
        ]

    def __str__(self):
        return f"{self.meter.serial_number} - {self.reading_date}: {self.reading_value}"
