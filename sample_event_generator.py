#!/usr/bin/env python3
"""
Sample event generator for testing the Event Hub Delta Pipeline.
This script generates sample events that can be used for testing the pipeline.
"""

import json
import uuid
from datetime import datetime, timedelta
import random
from typing import List, Dict, Any


class SampleEventGenerator:
    """Generates sample events for testing the pipeline."""
    
    def __init__(self):
        self.event_types = [
            "USER.LOGIN",
            "USER.LOGOUT", 
            "USER.REGISTRATION",
            "SYSTEM.STARTUP",
            "SYSTEM.SHUTDOWN",
            "ERROR.DATABASE_CONNECTION",
            "ERROR.AUTHENTICATION_FAILED",
            "SECURITY.SUSPICIOUS_ACTIVITY",
            "AUDIT.DATA_ACCESS",
            "AUDIT.CONFIGURATION_CHANGE"
        ]
        
        self.sources = [
            "https://app.example.com",
            "https://api.example.com",
            "system-service",
            "auth-service",
            "database-service",
            "security-monitor"
        ]
        
        self.subjects = [
            "user/authentication",
            "user/profile",
            "system/health",
            "system/configuration",
            "api/request",
            "database/connection",
            "security/audit"
        ]
    
    def generate_event(self, event_time: datetime = None) -> Dict[str, Any]:
        """Generate a single sample event."""
        if event_time is None:
            event_time = datetime.utcnow()
        
        event_type = random.choice(self.event_types)
        source = random.choice(self.sources)
        subject = random.choice(self.subjects)
        
        event_data = self._generate_event_data(event_type)
        
        event = {
            "eventId": str(uuid.uuid4()),
            "eventType": event_type,
            "eventTime": event_time.isoformat() + "Z",
            "source": source,
            "subject": subject,
            "data": json.dumps(event_data),
            "dataVersion": "1.0",
            "metadataVersion": "1"
        }
        
        return event
    
    def _generate_event_data(self, event_type: str) -> Dict[str, Any]:
        """Generate event-specific data based on event type."""
        base_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "correlationId": str(uuid.uuid4())
        }
        
        if "USER" in event_type:
            base_data.update({
                "userId": f"user_{random.randint(1000, 9999)}",
                "sessionId": str(uuid.uuid4()),
                "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "ipAddress": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
            })
            
            if event_type == "USER.LOGIN":
                base_data.update({
                    "action": "login",
                    "loginMethod": random.choice(["password", "sso", "mfa"]),
                    "success": random.choice([True, True, True, False])  # 75% success rate
                })
            elif event_type == "USER.REGISTRATION":
                base_data.update({
                    "action": "register",
                    "registrationSource": random.choice(["web", "mobile", "api"])
                })
        
        elif "SYSTEM" in event_type:
            base_data.update({
                "serviceId": f"service_{random.randint(1, 10)}",
                "version": f"1.{random.randint(0, 9)}.{random.randint(0, 9)}",
                "environment": random.choice(["production", "staging", "development"])
            })
            
            if event_type == "SYSTEM.STARTUP":
                base_data.update({
                    "action": "startup",
                    "startupTime": random.randint(5, 30)
                })
        
        elif "ERROR" in event_type:
            base_data.update({
                "errorCode": f"ERR_{random.randint(1000, 9999)}",
                "severity": random.choice(["low", "medium", "high", "critical"]),
                "component": random.choice(["database", "auth", "api", "frontend"])
            })
            
            if event_type == "ERROR.DATABASE_CONNECTION":
                base_data.update({
                    "action": "database_connection_failed",
                    "database": random.choice(["users", "orders", "inventory"]),
                    "retryCount": random.randint(1, 5)
                })
        
        elif "SECURITY" in event_type:
            base_data.update({
                "securityLevel": random.choice(["info", "warning", "alert"]),
                "riskScore": random.randint(1, 100)
            })
        
        elif "AUDIT" in event_type:
            base_data.update({
                "auditType": random.choice(["access", "modification", "deletion"]),
                "resource": random.choice(["user_data", "configuration", "logs"]),
                "actor": f"user_{random.randint(1000, 9999)}"
            })
        
        return base_data
    
    def generate_batch(self, count: int, time_range_minutes: int = 60) -> List[Dict[str, Any]]:
        """Generate a batch of sample events over a time range."""
        events = []
        start_time = datetime.utcnow() - timedelta(minutes=time_range_minutes)
        
        for i in range(count):
            event_time = start_time + timedelta(
                minutes=random.uniform(0, time_range_minutes)
            )
            event = self.generate_event(event_time)
            events.append(event)
        
        events.sort(key=lambda x: x["eventTime"])
        return events
    
    def generate_invalid_events(self, count: int = 5) -> List[Dict[str, Any]]:
        """Generate invalid events for testing error handling."""
        invalid_events = []
        
        for i in range(count):
            event = self.generate_event()
            
            if i == 0:
                event["eventId"] = ""
            elif i == 1:
                event["eventType"] = ""
            elif i == 2:
                event["eventTime"] = None
            elif i == 3:
                event["source"] = ""
            elif i == 4:
                event["data"] = "invalid json {"
            
            invalid_events.append(event)
        
        return invalid_events
    
    def save_events_to_file(self, events: List[Dict[str, Any]], filename: str):
        """Save events to a JSON file."""
        with open(filename, 'w') as f:
            json.dump(events, f, indent=2)
        print(f"Saved {len(events)} events to {filename}")
    
    def print_sample_events(self, events: List[Dict[str, Any]], count: int = 3):
        """Print a few sample events for inspection."""
        print(f"Sample events (showing {min(count, len(events))} of {len(events)}):")
        print("-" * 80)
        
        for i, event in enumerate(events[:count]):
            print(f"Event {i + 1}:")
            print(json.dumps(event, indent=2))
            print("-" * 80)


def main():
    """Generate sample events for testing."""
    print("Sample Event Generator for Event Hub Delta Pipeline")
    print("=" * 60)
    
    generator = SampleEventGenerator()
    
    print("Generating valid events...")
    valid_events = generator.generate_batch(count=100, time_range_minutes=120)
    generator.save_events_to_file(valid_events, "sample_valid_events.json")
    generator.print_sample_events(valid_events, count=2)
    
    print("\nGenerating invalid events...")
    invalid_events = generator.generate_invalid_events(count=10)
    generator.save_events_to_file(invalid_events, "sample_invalid_events.json")
    generator.print_sample_events(invalid_events, count=2)
    
    print("\nGenerating mixed batch...")
    mixed_events = valid_events[:50] + invalid_events
    random.shuffle(mixed_events)
    generator.save_events_to_file(mixed_events, "sample_mixed_events.json")
    
    print(f"\nGenerated event files:")
    print(f"  - sample_valid_events.json: {len(valid_events)} valid events")
    print(f"  - sample_invalid_events.json: {len(invalid_events)} invalid events")
    print(f"  - sample_mixed_events.json: {len(mixed_events)} mixed events")
    
    print("\nThese files can be used to test the pipeline components.")


if __name__ == "__main__":
    main()
