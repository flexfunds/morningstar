import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from pathlib import Path
import logging


class EmailSender:
    def __init__(self, smtp_config: dict):
        """
        Initialize EmailSender with SMTP configuration

        Args:
            smtp_config (dict): SMTP configuration containing:
                - host: SMTP server hostname
                - port: SMTP server port
                - user: SMTP username
                - password: SMTP password
                - use_tls: Boolean to indicate if TLS should be used
        """
        self.smtp_config = smtp_config
        self.logger = logging.getLogger(__name__)

    def send_report(self,
                    to_emails: list,
                    subject: str,
                    body: str,
                    attachment_path: Path) -> bool:
        """
        Send email with attachment

        Args:
            to_emails (list): List of recipient email addresses
            subject (str): Email subject
            body (str): Email body text
            attachment_path (Path): Path to the file to attach

        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        try:
            # Create message container
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['user']
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject

            # Add body
            msg.attach(MIMEText(body, 'plain'))

            # Add attachment
            if attachment_path.exists():
                with open(attachment_path, 'rb') as f:
                    attachment = MIMEApplication(f.read(), _subtype='xls')
                    attachment.add_header('Content-Disposition',
                                          'attachment',
                                          filename=attachment_path.name)
                    msg.attach(attachment)
            else:
                self.logger.error(f"Attachment file not found: {
                                  attachment_path}")
                return False

            # Connect to SMTP server and send email
            with smtplib.SMTP(self.smtp_config['host'], self.smtp_config['port']) as server:
                if self.smtp_config.get('use_tls', True):
                    server.starttls()
                server.login(self.smtp_config['user'],
                             self.smtp_config['password'])
                server.send_message(msg)

            self.logger.info(f"Email sent successfully to {
                             ', '.join(to_emails)}")
            return True

        except Exception as e:
            self.logger.error(f"Error sending email: {str(e)}")
            return False
