from django.contrib import admin
from django.db.models import Q
from django.utils.html import format_html
from .models import FlowFile, MeterPoint, Meter, Reading


@admin.register(FlowFile)
class FlowFileAdmin(admin.ModelAdmin):
    list_display = ['filename', 'status', 'row_count', 'imported_at']
    list_filter = ['status', 'imported_at']
    search_fields = ['filename']
    readonly_fields = ['file_hash', 'imported_at', 'row_count', 'error_message']
    
    def has_add_permission(self, request):
        return False


@admin.register(MeterPoint)
class MeterPointAdmin(admin.ModelAdmin):
    list_display = ['mpan', 'meter_count', 'created_at']
    search_fields = ['mpan']
    readonly_fields = ['created_at', 'updated_at']
    
    def meter_count(self, obj):
        return obj.meters.count()
    meter_count.short_description = 'Number of Meters'


@admin.register(Meter)
class MeterAdmin(admin.ModelAdmin):
    list_display = ['serial_number', 'meter_point_mpan', 'meter_type', 'reading_count']
    list_filter = ['meter_type', 'installed_date']
    search_fields = ['serial_number', 'meter_point__mpan']
    readonly_fields = ['created_at', 'updated_at']
    raw_id_fields = ['meter_point']
    
    def meter_point_mpan(self, obj):
        return obj.meter_point.mpan
    meter_point_mpan.short_description = 'MPAN'
    meter_point_mpan.admin_order_field = 'meter_point__mpan'
    
    def reading_count(self, obj):
        return obj.readings.count()
    reading_count.short_description = 'Number of Readings'


@admin.register(Reading)
class ReadingAdmin(admin.ModelAdmin):
    list_display = [
        'meter_serial', 
        'mpan', 
        'reading_date', 
        'reading_value', 
        'reading_type',
        'source_file'
    ]
    list_filter = ['reading_type', 'reading_date', 'flow_file']
    search_fields = ['meter__serial_number', 'meter__meter_point__mpan']
    date_hierarchy = 'reading_date'
    readonly_fields = ['created_at']
    raw_id_fields = ['meter', 'flow_file']
    
    def meter_serial(self, obj):
        return obj.meter.serial_number
    meter_serial.short_description = 'Meter Serial'
    meter_serial.admin_order_field = 'meter__serial_number'
    
    def mpan(self, obj):
        return obj.meter.meter_point.mpan
    mpan.short_description = 'MPAN'
    mpan.admin_order_field = 'meter__meter_point__mpan'
    
    def source_file(self, obj):
        return format_html(
            '<span title="{}">{}</span>',
            obj.flow_file.filename,
            obj.flow_file.filename[:30] + '...' if len(obj.flow_file.filename) > 30 else obj.flow_file.filename
        )
    source_file.short_description = 'Source File'
    
    def get_search_results(self, request, queryset, search_term):
        """Enhanced search to handle both MPAN and serial number"""
        queryset, use_distinct = super().get_search_results(request, queryset, search_term)
        
        if search_term:
            # If search term looks like MPAN (13 digits)
            if search_term.isdigit() and len(search_term) <= 13:
                queryset |= self.model.objects.filter(
                    meter__meter_point__mpan__startswith=search_term
                )
            # Otherwise search in serial numbers
            else:
                queryset |= self.model.objects.filter(
                    meter__serial_number__icontains=search_term
                )
        
        return queryset, use_distinct


# Customize admin site
admin.site.site_header = "G-Kraken Flow File Processor"
admin.site.site_title = "Flow File Admin"
admin.site.index_title = "Welcome to Flow File Administration"