from typing import Dict, Any, List
import logging
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

class QualityAlertSystem:
    """Monitor and alert on data quality issues."""
    
    def __init__(self, email_config: Dict[str, str]):
        self.email_config = email_config
        self.alert_thresholds = {
            'quality_score': 80.0,
            'missing_values': 20.0,
            'consistency': 90.0
        }
    
    def check_and_alert(self, quality_metrics: Dict[str, Any]) -> None:
        """Check metrics and send alerts if needed."""
        try:
            alerts = self.generate_alerts(quality_metrics)
            if alerts:
                self.send_alerts(alerts)
        except Exception as e:
            logger.error(f"Error in alert system: {str(e)}")
    
    def generate_alerts(self, metrics: Dict[str, Any]) -> List[str]:
        """Generate alert messages based on metrics."""
        alerts = []
        
        quality_score = metrics.get('quality_score', 0)
        if quality_score < self.alert_thresholds['quality_score']:
            alerts.append(
                f"Low quality score: {quality_score:.2f}% "
                f"(threshold: {self.alert_thresholds['quality_score']}%)"
            )
        
        missing_pct = metrics.get('missing_values', {}).get('percentage', 0)
        if missing_pct > self.alert_thresholds['missing_values']:
            alerts.append(
                f"High percentage of missing values: {missing_pct:.2f}% "
                f"(threshold: {self.alert_thresholds['missing_values']}%)"
            )
        
        return alerts
    
    def send_alerts(self, alerts: List[str]) -> None:
        """Send email alerts."""
        try:
            msg = MIMEMultipart()
            msg['Subject'] = f"Data Quality Alert - {datetime.now().isoformat()}"
            msg['From'] = self.email_config['from']
            msg['To'] = self.email_config['to']
            
            body = "\n".join([
                "Data Quality Alerts:",
                "-------------------",
                *alerts,
                "\nPlease investigate these issues."
            ])
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Create SMTP connection
            smtp = smtplib.SMTP(self.email_config['smtp_server'])
            smtp.starttls()
            smtp.login(
                self.email_config['username'],
                self.email_config['password']
            )
            smtp.send_message(msg)
            smtp.quit()
            
            logger.info("Quality alerts sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending alerts: {str(e)}") 