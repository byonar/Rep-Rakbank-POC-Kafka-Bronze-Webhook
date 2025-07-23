from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Union
from datetime import datetime
import json
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bronze DBO Trans HST2 Webhook Service",
    description="Confluent Kafka HTTP Sink Connector Webhook for bronze_dbo_trans_hst2 Topic",
    version="2.0.0"
)

# Bronze Trans HST2 Data Model (21 fields from Delta table)
class BronzeTransHst2(BaseModel):
    authorizer_usrnbr: Optional[int] = Field(default=None, description="Authorizer user number")
    creat_time: Optional[str] = Field(default=None, description="Creation timestamp")
    creat_usrnbr: Optional[int] = Field(default=None, description="Creator user number")
    data: Optional[str] = Field(default=None, description="Transaction data")
    description: Optional[str] = Field(default=None, description="Description")
    description2: Optional[str] = Field(default=None, description="Secondary description")
    external_user: Optional[str] = Field(default=None, description="External user")
    four_eye_on: Optional[int] = Field(default=None, description="Four eye flag")
    name: Optional[str] = Field(default=None, description="Name")
    next_seqnbr: Optional[int] = Field(default=None, description="Next sequence number")
    oper: Optional[int] = Field(default=None, description="Operation")
    owner_usrnbr: Optional[int] = Field(default=None, description="Owner user number")
    protection: Optional[int] = Field(default=None, description="Protection level")
    record_id: Optional[int] = Field(default=None, description="Record ID")
    seqnbr: Optional[int] = Field(default=None, description="Sequence number")
    size: Optional[int] = Field(default=None, description="Size")
    transnbr: Optional[int] = Field(default=None, description="Transaction number")
    trans_record_type: Optional[int] = Field(default=None, description="Transaction record type")
    updat_time: Optional[str] = Field(default=None, description="Update timestamp")
    updat_usrnbr: Optional[int] = Field(default=None, description="Updater user number")
    version: Optional[int] = Field(default=None, description="Version")
    
    # Metadata
    received_at: datetime = Field(default_factory=datetime.now)
    processing_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

class BronzeResponse(BaseModel):
    total_received: int
    last_10_records: List[BronzeTransHst2]
    last_updated: datetime
    service_status: str = "active"
    topic_name: str = "bronze_dbo_trans_hst2"

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    timestamp: datetime
    total_processed: int
    uptime_info: str

# In-memory storage
record_counter = 0
bronze_storage: List[BronzeTransHst2] = []

# Message Parser for Confluent HTTP Sink Connector
class ConfluentMessageParser:
    
    @staticmethod
    def parse_confluent_message(raw_data: str) -> BronzeTransHst2:
        """Parse message from Confluent HTTP Sink Connector"""
        try:
            logger.info(f"Parsing Confluent message: {len(raw_data)} characters")
            
            # Try JSON parsing first
            try:
                json_data = json.loads(raw_data)
                return ConfluentMessageParser._parse_json_message(json_data)
            except json.JSONDecodeError:
                # If not JSON, try AVRO string parsing
                return ConfluentMessageParser._parse_avro_string(raw_data)
                
        except Exception as e:
            logger.error(f"Message parsing failed: {e}")
            return ConfluentMessageParser._create_error_record(raw_data, str(e))
    
    @staticmethod
    def _parse_json_message(json_data: dict) -> BronzeTransHst2:
        """Parse JSON format message"""
        
        # Handle different JSON structures from Confluent
        if 'value' in json_data:
            data = json_data['value']
        elif 'payload' in json_data:
            data = json_data['payload']
        else:
            data = json_data
        
        # Convert timestamp fields (handle both string and long formats)
        creat_time = ConfluentMessageParser._convert_timestamp(data.get('creat_time'))
        updat_time = ConfluentMessageParser._convert_timestamp(data.get('updat_time'))
        
        return BronzeTransHst2(
            authorizer_usrnbr=data.get('authorizer_usrnbr'),
            creat_time=creat_time,
            creat_usrnbr=data.get('creat_usrnbr'),
            data=data.get('data'),
            description=data.get('description'),
            description2=data.get('description2'),
            external_user=data.get('external_user'),
            four_eye_on=data.get('four_eye_on'),
            name=data.get('name'),
            next_seqnbr=data.get('next_seqnbr'),
            oper=data.get('oper'),
            owner_usrnbr=data.get('owner_usrnbr'),
            protection=data.get('protection'),
            record_id=data.get('record_id'),
            seqnbr=data.get('seqnbr'),
            size=data.get('size'),
            transnbr=data.get('transnbr'),
            trans_record_type=data.get('trans_record_type'),
            updat_time=updat_time,
            updat_usrnbr=data.get('updat_usrnbr'),
            version=data.get('version')
        )
    
    @staticmethod
    def _parse_avro_string(avro_string: str) -> BronzeTransHst2:
        """Parse AVRO string format (fallback method)"""
        import re
        
        def extract_value(field_name: str) -> Union[str, int, None]:
            # Try different AVRO patterns
            patterns = [
                rf'"{field_name}"\s*:\s*{{\s*"int"\s*:\s*(\d+)\s*}}',  # {"int": 123}
                rf'"{field_name}"\s*:\s*{{\s*"string"\s*:\s*"([^"]+)"\s*}}',  # {"string": "value"}
                rf'"{field_name}"\s*:\s*(\d+)',  # direct number
                rf'"{field_name}"\s*:\s*"([^"]+)"'  # direct string
            ]
            
            for pattern in patterns:
                match = re.search(pattern, avro_string)
                if match:
                    value = match.group(1)
                    # Try to convert to int if it's a number
                    try:
                        return int(value)
                    except ValueError:
                        return value
            return None
        
        return BronzeTransHst2(
            authorizer_usrnbr=extract_value('authorizer_usrnbr'),
            creat_time=extract_value('creat_time'),
            creat_usrnbr=extract_value('creat_usrnbr'),
            data=extract_value('data'),
            description=extract_value('description'),
            description2=extract_value('description2'),
            external_user=extract_value('external_user'),
            four_eye_on=extract_value('four_eye_on'),
            name=extract_value('name'),
            next_seqnbr=extract_value('next_seqnbr'),
            oper=extract_value('oper'),
            owner_usrnbr=extract_value('owner_usrnbr'),
            protection=extract_value('protection'),
            record_id=extract_value('record_id'),
            seqnbr=extract_value('seqnbr'),
            size=extract_value('size'),
            transnbr=extract_value('transnbr'),
            trans_record_type=extract_value('trans_record_type'),
            updat_time=extract_value('updat_time'),
            updat_usrnbr=extract_value('updat_usrnbr'),
            version=extract_value('version')
        )
    
    @staticmethod
    def _convert_timestamp(timestamp_value) -> Optional[str]:
        """Convert timestamp from various formats to ISO string"""
        if not timestamp_value:
            return None
            
        try:
            # If it's already a string, return it
            if isinstance(timestamp_value, str):
                return timestamp_value
            
            # If it's a timestamp in milliseconds, convert it
            if isinstance(timestamp_value, (int, float)):
                if timestamp_value > 1e12:  # Milliseconds
                    dt = datetime.fromtimestamp(timestamp_value / 1000)
                else:  # Seconds
                    dt = datetime.fromtimestamp(timestamp_value)
                return dt.isoformat()
                
        except Exception as e:
            logger.warning(f"Timestamp conversion failed: {e}")
            
        return str(timestamp_value)
    
    @staticmethod
    def _create_error_record(raw_data: str, error_msg: str) -> BronzeTransHst2:
        """Create error record when parsing fails"""
        return BronzeTransHst2(
            record_id=-1,
            data=f"PARSE_ERROR: {error_msg}",
            description=raw_data[:100] + ("..." if len(raw_data) > 100 else ""),
            name="PARSING_FAILED",
            creat_time=datetime.now().isoformat()
        )

# Helper function for dashboard
def _generate_records_html(records: List[BronzeTransHst2]) -> str:
    """Generate HTML for records list"""
    if not records:
        return '<div class="no-records">📭 No records received yet. Waiting for Kafka messages...</div>'
    
    html = ""
    for idx, record in enumerate(records):
        # Format received time
        received_time = record.received_at.strftime('%Y-%m-%d %H:%M:%S') if record.received_at else 'Unknown'
        
        # Create field list (only show non-null values)
        fields_html = ""
        field_data = [
            ("Record ID", record.record_id),
            ("Transaction Number", record.transnbr),
            ("Sequence Number", record.seqnbr),
            ("Creator User", record.creat_usrnbr),
            ("Create Time", record.creat_time),
            ("Update Time", record.updat_time),
            ("Authorizer User", record.authorizer_usrnbr),
            ("Owner User", record.owner_usrnbr),
            ("Name", record.name),
            ("Description", record.description),
            ("Description 2", record.description2),
            ("Data", record.data),
            ("External User", record.external_user),
            ("Operation", record.oper),
            ("Protection", record.protection),
            ("Four Eye On", record.four_eye_on),
            ("Next Seq Number", record.next_seqnbr),
            ("Size", record.size),
            ("Trans Record Type", record.trans_record_type),
            ("Update User", record.updat_usrnbr),
            ("Version", record.version)
        ]
        
        for field_name, field_value in field_data:
            if field_value is not None:
                # Truncate long values
                display_value = str(field_value)
                if len(display_value) > 50:
                    display_value = display_value[:50] + "..."
                
                fields_html += f'''
                <div class="field">
                    <span class="field-name">{field_name}:</span>
                    <span class="field-value">{display_value}</span>
                </div>
                '''
        
        html += f'''
        <div class="record-card">
            <div class="record-header">
                <span class="record-id">#{record.record_id or 'N/A'}</span>
                <span class="received-time">📅 {received_time}</span>
            </div>
            <div class="record-fields">
                {fields_html}
            </div>
        </div>
        '''
    
    return html

# API Endpoints
@app.post("/webhook/bronze-trans-hst2")
async def receive_bronze_record(request: Request):
    """Receive bronze_dbo_trans_hst2 record from Confluent HTTP Sink Connector"""
    global record_counter, bronze_storage
    
    try:
        # Get raw body
        raw_body = await request.body()
        raw_data = raw_body.decode('utf-8')
        
        logger.info(f"Received bronze record: {len(raw_data)} chars")
        
        # Parse the message
        bronze_record = ConfluentMessageParser.parse_confluent_message(raw_data)
        
        # Store record
        bronze_storage.append(bronze_record)
        record_counter += 1
        
        # Keep only last 10 records
        if len(bronze_storage) > 10:
            bronze_storage = bronze_storage[-10:]
        
        logger.info(f"Bronze record processed. Total: {record_counter}, Record ID: {bronze_record.record_id}")
        
        return {
            "status": "success",
            "message": "Bronze record received successfully",
            "total_count": record_counter,
            "record_id": bronze_record.record_id,
            "processing_id": bronze_record.processing_id,
            "received_at": bronze_record.received_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing bronze record: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to process record: {str(e)}")

@app.get("/webhook/bronze-trans-hst2")
async def get_bronze_records():
    """Get bronze record statistics and last 10 records"""
    global record_counter, bronze_storage
    
    # Reverse to show newest first
    last_records = list(reversed(bronze_storage))
    
    response = BronzeResponse(
        total_received=record_counter,
        last_10_records=last_records,
        last_updated=datetime.now()
    )
    
    return response

@app.get("/webhook/bronze-trans-hst2/stats")
async def get_bronze_stats():
    """Get detailed statistics about received records"""
    global record_counter, bronze_storage
    
    # Calculate some statistics
    record_ids = [r.record_id for r in bronze_storage if r.record_id]
    unique_users = set([r.creat_usrnbr for r in bronze_storage if r.creat_usrnbr])
    
    return {
        "total_records": record_counter,
        "current_storage": len(bronze_storage),
        "unique_record_ids": len(set(record_ids)) if record_ids else 0,
        "unique_users": len(unique_users),
        "last_record_id": max(record_ids) if record_ids else None,
        "service_uptime": "active",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Web dashboard showing last 10 records"""
    global record_counter, bronze_storage
    
    # Get last 10 records (newest first)
    last_records = list(reversed(bronze_storage))
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Bronze DBO Trans HST2 - Dashboard</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: #333;
                min-height: 100vh;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                overflow: hidden;
            }}
            .header {{
                background: #2c3e50;
                color: white;
                padding: 20px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 24px;
            }}
            .stats {{
                display: flex;
                background: #ecf0f1;
                padding: 15px;
                justify-content: space-around;
                border-bottom: 1px solid #bdc3c7;
            }}
            .stat-item {{
                text-align: center;
            }}
            .stat-number {{
                font-size: 24px;
                font-weight: bold;
                color: #27ae60;
            }}
            .stat-label {{
                font-size: 12px;
                color: #7f8c8d;
                text-transform: uppercase;
            }}
            .records-section {{
                padding: 20px;
            }}
            .records-title {{
                margin: 0 0 20px 0;
                color: #2c3e50;
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
            }}
            .record-card {{
                background: #f8f9fa;
                border: 1px solid #e9ecef;
                border-radius: 8px;
                margin-bottom: 15px;
                padding: 15px;
                transition: transform 0.2s;
            }}
            .record-card:hover {{
                transform: translateY(-2px);
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }}
            .record-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
                padding-bottom: 8px;
                border-bottom: 1px solid #dee2e6;
            }}
            .record-id {{
                font-weight: bold;
                color: #e74c3c;
                font-size: 16px;
            }}
            .received-time {{
                color: #7f8c8d;
                font-size: 12px;
            }}
            .record-fields {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 8px;
            }}
            .field {{
                font-size: 12px;
            }}
            .field-name {{
                font-weight: bold;
                color: #34495e;
            }}
            .field-value {{
                color: #2c3e50;
                margin-left: 5px;
            }}
            .no-records {{
                text-align: center;
                color: #7f8c8d;
                padding: 40px;
                font-style: italic;
            }}
            .refresh-btn {{
                position: fixed;
                bottom: 20px;
                right: 20px;
                background: #3498db;
                color: white;
                border: none;
                border-radius: 50px;
                padding: 15px 20px;
                cursor: pointer;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                font-weight: bold;
            }}
            .refresh-btn:hover {{
                background: #2980b9;
            }}
            .status-badge {{
                background: #27ae60;
                color: white;
                padding: 4px 8px;
                border-radius: 12px;
                font-size: 10px;
                text-transform: uppercase;
            }}
        </style>
        <script>
            function refreshPage() {{
                window.location.reload();
            }}
            
            // Auto refresh every 30 seconds
            setInterval(refreshPage, 30000);
        </script>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏦 Bronze DBO Trans HST2 Dashboard</h1>
                <span class="status-badge">Live Monitoring</span>
            </div>
            
            <div class="stats">
                <div class="stat-item">
                    <div class="stat-number">{record_counter}</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{len(bronze_storage)}</div>
                    <div class="stat-label">In Memory</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">{datetime.now().strftime('%H:%M:%S')}</div>
                    <div class="stat-label">Last Update</div>
                </div>
                <div class="stat-item">
                    <div class="stat-number">30s</div>
                    <div class="stat-label">Auto Refresh</div>
                </div>
            </div>
            
            <div class="records-section">
                <h2 class="records-title">📊 Last 10 Records (Newest First)</h2>
                
                {_generate_records_html(last_records)}
            </div>
        </div>
        
        <button class="refresh-btn" onclick="refreshPage()">🔄 Refresh Now</button>
    </body>
    </html>
    """
    
    return html_content

@app.post("/webhook/bronze-trans-hst2/reset")
async def reset_bronze_counters():
    """Reset bronze record counters and storage"""
    global record_counter, bronze_storage
    
    old_count = record_counter
    record_counter = 0
    bronze_storage = []
    
    logger.info(f"Bronze counters reset. Previous count: {old_count}")
    
    return {
        "status": "success",
        "message": f"Counters reset successfully. Previous count: {old_count}",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        service="bronze-dbo-trans-hst2-webhook",
        version="2.0.0",
        timestamp=datetime.now(),
        total_processed=record_counter,
        uptime_info="running"
    )

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Bronze DBO Trans HST2 Webhook Service",
        "version": "2.0.0",
        "kafka_topic": "bronze_dbo_trans_hst2",
        "connector_type": "confluent_http_sink",
        "endpoints": {
            "dashboard": "/dashboard",
            "webhook_post": "/webhook/bronze-trans-hst2",
            "webhook_get": "/webhook/bronze-trans-hst2",
            "stats": "/webhook/bronze-trans-hst2/stats",
            "reset": "/webhook/bronze-trans-hst2/reset",
            "health": "/health",
            "docs": "/docs"
        },
        "azure_deployment": "ready",
        "github_integration": "supported"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)