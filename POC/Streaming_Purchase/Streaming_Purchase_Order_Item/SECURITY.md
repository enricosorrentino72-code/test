# Security Documentation

This document outlines security practices, vulnerability scanning, and defensive security measures for the Purchase Order Item Streaming Pipeline.

## 🛡️ Security Overview

### Security Objectives
- **Data Protection**: Secure handling of purchase order data
- **Access Control**: Proper authentication and authorization
- **Vulnerability Management**: Proactive identification and remediation
- **Supply Chain Security**: Secure dependencies and build process
- **Runtime Security**: Secure execution environment

### Security Principles
- **Defense in Depth**: Multiple layers of security controls
- **Least Privilege**: Minimal required permissions
- **Zero Trust**: Verify everything, trust nothing
- **Secure by Default**: Security-first configuration
- **Continuous Monitoring**: Real-time security monitoring

## 🔍 Static Application Security Testing (SAST)

### 1. Bandit - Python Security Linter

#### Configuration
```bash
# Install Bandit
pip install bandit[toml]

# Run security scan
bandit -r class utility -f json -o bandit-report.json

# High confidence issues only
bandit -r class utility -ll

# With configuration file
bandit -r . -c pyproject.toml
```

#### pyproject.toml Configuration
```toml
[tool.bandit]
exclude_dirs = ["tests", "test", ".venv", "venv"]
skips = ["B101", "B601"]  # Skip assert_used and shell_injection tests
```

#### Common Security Issues Detected
- **B108**: Hardcoded temporary file/directory
- **B110**: Except: pass (potential security risk)
- **B303**: Use of insecure MD2, MD4, MD5 hash functions
- **B501**: Request with verify=False (SSL verification disabled)
- **B601**: Shell injection vulnerabilities
- **B602**: Subprocess with shell=True

#### Example Secure Code
```python
# ❌ Insecure: Hardcoded credentials
def connect_to_database():
    password = "hardcoded_password"
    return connect(password=password)

# ✅ Secure: Environment variables
import os
def connect_to_database():
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise ValueError("DB_PASSWORD environment variable not set")
    return connect(password=password)
```

### 2. Safety - Dependency Vulnerability Scanner

#### Installation and Usage
```bash
# Install Safety
pip install safety

# Check for vulnerabilities
safety check

# Generate JSON report
safety check --json --output safety-report.json

# Check specific requirements file
safety check -r requirements.txt

# Check with full report
safety check --full-report
```

#### Example Vulnerability Report
```json
{
  "vulnerabilities": [
    {
      "advisory": "Insecure dependency version",
      "cve": "CVE-2023-12345",
      "id": "12345",
      "specs": ["<1.2.3"],
      "v": "1.2.2"
    }
  ]
}
```

### 3. pip-audit - Supply Chain Security

#### Installation and Usage
```bash
# Install pip-audit
pip install pip-audit

# Audit installed packages
pip-audit

# Generate JSON report
pip-audit --format=json --output=pip-audit-report.json

# Fix vulnerabilities automatically
pip-audit --fix

# Audit specific requirements file
pip-audit -r requirements.txt
```

### 4. Semgrep - Advanced SAST

#### Installation and Usage
```bash
# Install Semgrep
pip install semgrep

# Run default security rules
semgrep --config=auto class utility

# Run specific rulesets
semgrep --config=p/python class utility

# Generate JSON report
semgrep --config=auto --json --output=semgrep-report.json class utility
```

## 🔐 Authentication & Authorization

### Azure Active Directory Integration
```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

class SecureConfigManager:
    """Secure configuration management using Azure Key Vault."""

    def __init__(self, vault_url: str):
        self.credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=vault_url, credential=self.credential)

    def get_secret(self, secret_name: str) -> str:
        """Retrieve secret from Key Vault."""
        try:
            secret = self.client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_name}: {e}")
            raise
```

### Service Principal Authentication
```python
from azure.identity import ClientSecretCredential

def get_service_principal_credential():
    """Get service principal credentials securely."""
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError("Missing required Azure credentials")

    return ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
```

## 🔒 Data Protection

### Encryption at Rest
```python
from cryptography.fernet import Fernet
import os

class DataEncryption:
    """Data encryption utilities for sensitive information."""

    def __init__(self):
        self.key = self._get_encryption_key()
        self.cipher = Fernet(self.key)

    def _get_encryption_key(self) -> bytes:
        """Get encryption key from environment or generate new one."""
        key_str = os.getenv("ENCRYPTION_KEY")
        if key_str:
            return key_str.encode()
        else:
            # Generate new key (store securely!)
            return Fernet.generate_key()

    def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data."""
        return self.cipher.encrypt(data.encode()).decode()

    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data."""
        return self.cipher.decrypt(encrypted_data.encode()).decode()
```

### Secure Data Transmission
```python
import ssl
from azure.eventhub import EventHubProducerClient

def create_secure_eventhub_client(connection_string: str) -> EventHubProducerClient:
    """Create EventHub client with secure SSL configuration."""
    # Create SSL context with secure settings
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = True
    ssl_context.verify_mode = ssl.CERT_REQUIRED

    # Create client with SSL context
    client = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        ssl_context=ssl_context
    )

    return client
```

### PII Data Handling
```python
import re
from typing import Dict, Any

class PIIDataMasker:
    """Mask personally identifiable information in data."""

    @staticmethod
    def mask_customer_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Mask sensitive customer information."""
        masked_data = data.copy()

        # Mask email addresses
        if 'email' in masked_data:
            email = masked_data['email']
            masked_data['email'] = PIIDataMasker._mask_email(email)

        # Mask phone numbers
        if 'phone' in masked_data:
            phone = masked_data['phone']
            masked_data['phone'] = PIIDataMasker._mask_phone(phone)

        # Mask credit card numbers
        if 'credit_card' in masked_data:
            cc = masked_data['credit_card']
            masked_data['credit_card'] = PIIDataMasker._mask_credit_card(cc)

        return masked_data

    @staticmethod
    def _mask_email(email: str) -> str:
        """Mask email address."""
        if '@' in email:
            local, domain = email.split('@', 1)
            return f"{local[:2]}***@{domain}"
        return "***"

    @staticmethod
    def _mask_phone(phone: str) -> str:
        """Mask phone number."""
        digits_only = re.sub(r'\D', '', phone)
        if len(digits_only) >= 4:
            return f"***-***-{digits_only[-4:]}"
        return "***"

    @staticmethod
    def _mask_credit_card(cc: str) -> str:
        """Mask credit card number."""
        digits_only = re.sub(r'\D', '', cc)
        if len(digits_only) >= 4:
            return f"****-****-****-{digits_only[-4:]}"
        return "****"
```

## 🚨 Security Monitoring

### Logging Security Events
```python
import structlog
from datetime import datetime

class SecurityLogger:
    """Security-focused logging utilities."""

    def __init__(self):
        self.logger = structlog.get_logger("security")

    def log_authentication_attempt(self, user_id: str, success: bool, ip_address: str):
        """Log authentication attempts."""
        self.logger.info(
            "authentication_attempt",
            user_id=user_id,
            success=success,
            ip_address=ip_address,
            timestamp=datetime.utcnow().isoformat()
        )

    def log_data_access(self, user_id: str, resource: str, action: str):
        """Log data access events."""
        self.logger.info(
            "data_access",
            user_id=user_id,
            resource=resource,
            action=action,
            timestamp=datetime.utcnow().isoformat()
        )

    def log_security_violation(self, violation_type: str, details: str, severity: str):
        """Log security violations."""
        self.logger.warning(
            "security_violation",
            violation_type=violation_type,
            details=details,
            severity=severity,
            timestamp=datetime.utcnow().isoformat()
        )
```

### Input Validation
```python
from typing import Any, Dict
import re

class InputValidator:
    """Validate and sanitize user inputs."""

    @staticmethod
    def validate_order_id(order_id: str) -> bool:
        """Validate order ID format."""
        if not isinstance(order_id, str):
            return False
        # Must match pattern: ORD-XXXXXX
        pattern = r'^ORD-\d{6}$'
        return bool(re.match(pattern, order_id))

    @staticmethod
    def validate_product_id(product_id: str) -> bool:
        """Validate product ID format."""
        if not isinstance(product_id, str):
            return False
        # Must match pattern: PROD-XXX
        pattern = r'^PROD-\w{3,10}$'
        return bool(re.match(pattern, product_id))

    @staticmethod
    def sanitize_string_input(input_str: str, max_length: int = 255) -> str:
        """Sanitize string input to prevent injection attacks."""
        if not isinstance(input_str, str):
            raise ValueError("Input must be a string")

        # Remove potentially dangerous characters
        sanitized = re.sub(r'[<>&"\'`]', '', input_str)

        # Limit length
        sanitized = sanitized[:max_length]

        # Strip whitespace
        sanitized = sanitized.strip()

        return sanitized

    @staticmethod
    def validate_purchase_order_data(data: Dict[str, Any]) -> Dict[str, str]:
        """Validate purchase order data and return errors."""
        errors = {}

        # Required fields
        required_fields = ['order_id', 'product_id', 'quantity', 'unit_price']
        for field in required_fields:
            if field not in data or data[field] is None:
                errors[field] = f"{field} is required"

        # Validate order ID
        if 'order_id' in data and not InputValidator.validate_order_id(data['order_id']):
            errors['order_id'] = "Invalid order ID format"

        # Validate product ID
        if 'product_id' in data and not InputValidator.validate_product_id(data['product_id']):
            errors['product_id'] = "Invalid product ID format"

        # Validate numeric fields
        if 'quantity' in data:
            try:
                quantity = int(data['quantity'])
                if quantity <= 0:
                    errors['quantity'] = "Quantity must be positive"
            except (ValueError, TypeError):
                errors['quantity'] = "Quantity must be a valid integer"

        if 'unit_price' in data:
            try:
                price = float(data['unit_price'])
                if price <= 0:
                    errors['unit_price'] = "Unit price must be positive"
            except (ValueError, TypeError):
                errors['unit_price'] = "Unit price must be a valid number"

        return errors
```

## 🔄 Security in CI/CD Pipeline

### GitHub Actions Security Workflow
```yaml
name: Security Scan

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  security-scan:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        pip install bandit safety pip-audit semgrep

    - name: Run Bandit security scan
      run: |
        bandit -r class utility -f json -o bandit-report.json
        bandit -r class utility -ll  # High confidence only

    - name: Run Safety dependency check
      run: |
        safety check --json --output safety-report.json

    - name: Run pip-audit
      run: |
        pip-audit --format=json --output=pip-audit-report.json

    - name: Run Semgrep
      run: |
        semgrep --config=auto --json --output=semgrep-report.json class utility

    - name: Upload security reports
      uses: actions/upload-artifact@v3
      with:
        name: security-reports
        path: |
          bandit-report.json
          safety-report.json
          pip-audit-report.json
          semgrep-report.json

    - name: Fail on high severity issues
      run: |
        # Parse reports and fail if high severity issues found
        python scripts/check_security_issues.py
```

### Secrets Management
```yaml
# .github/workflows/security.yml
env:
  AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
  AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
  AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - name: Azure Login
      uses: azure/login@v1
      with:
        creds: ${{ secrets.AZURE_CREDENTIALS }}
```

## 📊 Security Metrics and Reporting

### Security Dashboard
```python
class SecurityDashboard:
    """Generate security metrics and reports."""

    def __init__(self):
        self.metrics = {
            'vulnerabilities': 0,
            'security_events': 0,
            'access_violations': 0,
            'last_scan': None
        }

    def generate_security_report(self) -> Dict[str, Any]:
        """Generate comprehensive security report."""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'vulnerability_summary': self._get_vulnerability_summary(),
            'access_summary': self._get_access_summary(),
            'compliance_status': self._get_compliance_status(),
            'recommendations': self._get_security_recommendations()
        }

    def _get_vulnerability_summary(self) -> Dict[str, int]:
        """Get vulnerability counts by severity."""
        return {
            'critical': 0,
            'high': 2,
            'medium': 5,
            'low': 8,
            'info': 12
        }

    def _get_compliance_status(self) -> Dict[str, bool]:
        """Check compliance with security standards."""
        return {
            'soc2_compliant': True,
            'gdpr_compliant': True,
            'hipaa_compliant': False,  # Not applicable
            'encryption_enabled': True,
            'access_logs_enabled': True
        }
```

## 🚨 Incident Response

### Security Incident Handling
```python
from enum import Enum
from dataclasses import dataclass
from datetime import datetime

class IncidentSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class SecurityIncident:
    incident_id: str
    severity: IncidentSeverity
    description: str
    affected_systems: List[str]
    detected_at: datetime
    reported_by: str
    status: str = "open"

class IncidentResponseManager:
    """Manage security incident response."""

    def __init__(self):
        self.incidents = []
        self.logger = structlog.get_logger("incident_response")

    def report_incident(self, incident: SecurityIncident):
        """Report a new security incident."""
        self.incidents.append(incident)
        self.logger.critical(
            "security_incident_reported",
            incident_id=incident.incident_id,
            severity=incident.severity.value,
            description=incident.description,
            affected_systems=incident.affected_systems
        )

        # Auto-escalate critical incidents
        if incident.severity == IncidentSeverity.CRITICAL:
            self._escalate_incident(incident)

    def _escalate_incident(self, incident: SecurityIncident):
        """Escalate critical security incidents."""
        # Send alerts to security team
        # Implement emergency response procedures
        self.logger.critical(
            "incident_escalated",
            incident_id=incident.incident_id,
            escalation_reason="critical_severity"
        )
```

## 📋 Security Checklist

### Development Security Checklist
- [ ] All secrets stored in environment variables or Key Vault
- [ ] Input validation implemented for all user inputs
- [ ] SQL injection prevention measures in place
- [ ] Cross-site scripting (XSS) prevention implemented
- [ ] Authentication and authorization properly configured
- [ ] Encryption used for sensitive data at rest and in transit
- [ ] Security headers configured
- [ ] Error messages don't expose sensitive information
- [ ] Logging captures security-relevant events
- [ ] Dependencies regularly updated and scanned for vulnerabilities

### Deployment Security Checklist
- [ ] Security scanning integrated into CI/CD pipeline
- [ ] Production secrets properly secured
- [ ] Network security controls configured
- [ ] Access controls and permissions reviewed
- [ ] Monitoring and alerting configured
- [ ] Backup and disaster recovery procedures tested
- [ ] Security incident response plan documented
- [ ] Compliance requirements verified

## 📚 Security Resources

### Standards and Frameworks
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [ISO 27001](https://www.iso.org/isoiec-27001-information-security.html)
- [SOC 2](https://www.aicpa.org/interestareas/frc/assuranceadvisoryservices/aicpasoc2report.html)

### Tools and Resources
- [Bandit Documentation](https://bandit.readthedocs.io/)
- [Safety Documentation](https://pyup.io/safety/)
- [Semgrep Documentation](https://semgrep.dev/docs/)
- [Azure Security Center](https://azure.microsoft.com/en-us/services/security-center/)

### Training
- Secure Coding Practices
- OWASP Security Training
- Azure Security Fundamentals
- Incident Response Training