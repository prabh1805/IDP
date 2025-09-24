#!/usr/bin/env python3
"""
email_trigger.py

Email integration that:
1. Monitors email inbox for emails with subject "Document Analysis"
2. Downloads PDF attachments from those emails
3. Triggers the pipeline process (uploadToS3 -> processing)
4. Optionally sends confirmation email back
"""

import imaplib
import email
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
import subprocess
import logging
from datetime import datetime

# --------------------------------------------------
# EMAIL CONFIGURATION
# --------------------------------------------------
EMAIL_CONFIG = {
    'imap_server': 'imap.gmail.com',  # Change for your email provider
    'imap_port': 993,
    'smtp_server': 'smtp.gmail.com',  # Change for your email provider
    'smtp_port': 587,
    'email': '',  # Your email address
    'password': '',  # Your email password or app password
    'check_interval': 60,  # Check every 60 seconds
    'subject_filter': 'Document Analysis',
    'processed_folder': 'INBOX/Processed',  # Optional: move processed emails here
}

# --------------------------------------------------
# LOGGING SETUP
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('email_trigger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# EMAIL FUNCTIONS
# --------------------------------------------------
class EmailProcessor:
    def __init__(self, config):
        self.config = config
        self.imap = None
        self.smtp = None
        
    def connect_imap(self):
        """Connect to IMAP server"""
        try:
            self.imap = imaplib.IMAP4_SSL(self.config['imap_server'], self.config['imap_port'])
            self.imap.login(self.config['email'], self.config['password'])
            self.imap.select('INBOX')
            logger.info("✅ Connected to IMAP server")
            return True
        except Exception as e:
            logger.error(f"❌ IMAP connection failed: {e}")
            return False
    
    def connect_smtp(self):
        """Connect to SMTP server for sending replies"""
        try:
            self.smtp = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            self.smtp.starttls()
            self.smtp.login(self.config['email'], self.config['password'])
            logger.info("✅ Connected to SMTP server")
            return True
        except Exception as e:
            logger.error(f"❌ SMTP connection failed: {e}")
            return False
    
    def search_emails(self):
        """Search for unread emails with specific subject"""
        try:
            # Search for unread emails with the target subject
            search_criteria = f'(UNSEEN SUBJECT "{self.config["subject_filter"]}")'
            status, messages = self.imap.search(None, search_criteria)
            
            if status != 'OK':
                logger.error("❌ Email search failed")
                return []
            
            email_ids = messages[0].split()
            logger.info(f"Found {len(email_ids)} unread emails with subject '{self.config['subject_filter']}'")
            return email_ids
            
        except Exception as e:
            logger.error(f"❌ Email search error: {e}")
            return []
    
    def process_email(self, email_id):
        """Process a single email"""
        try:
            # Fetch the email
            status, msg_data = self.imap.fetch(email_id, '(RFC822)')
            if status != 'OK':
                logger.error(f"❌ Failed to fetch email {email_id}")
                return False
            
            # Parse the email
            email_body = msg_data[0][1]
            email_message = email.message_from_bytes(email_body)
            
            # Extract email details
            sender = email_message['From']
            subject = email_message['Subject']
            date = email_message['Date']
            
            logger.info(f"📧 Processing email from {sender}")
            logger.info(f"   Subject: {subject}")
            logger.info(f"   Date: {date}")
            
            # Download PDF attachments
            pdf_files = self.download_attachments(email_message, email_id.decode())
            
            if pdf_files:
                # Process the PDFs
                success = self.trigger_pipeline(pdf_files, sender)
                
                if success:
                    # Mark email as read and optionally move to processed folder
                    self.mark_as_processed(email_id)
                    
                    # Send confirmation email
                    self.send_confirmation(sender, subject, len(pdf_files))
                    
                    return True
            else:
                logger.warning(f"⚠️ No PDF attachments found in email from {sender}")
                self.send_error_notification(sender, "No PDF attachments found")
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error processing email {email_id}: {e}")
            return False
    
    def download_attachments(self, email_message, email_id):
        """Download PDF attachments from email"""
        pdf_files = []
        attachment_dir = Path("email_attachments")
        attachment_dir.mkdir(exist_ok=True)
        
        try:
            for part in email_message.walk():
                if part.get_content_disposition() == 'attachment':
                    filename = part.get_filename()
                    if filename and filename.lower().endswith('.pdf'):
                        # Create unique filename
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        safe_filename = f"{timestamp}_{email_id}_{filename}"
                        filepath = attachment_dir / safe_filename
                        
                        # Save the attachment
                        with open(filepath, 'wb') as f:
                            f.write(part.get_payload(decode=True))
                        
                        pdf_files.append(filepath)
                        logger.info(f"📎 Downloaded attachment: {safe_filename}")
            
            return pdf_files
            
        except Exception as e:
            logger.error(f"❌ Error downloading attachments: {e}")
            return []
    
    def trigger_pipeline(self, pdf_files, sender):
        """Trigger the pipeline process for downloaded PDFs"""
        try:
            for pdf_file in pdf_files:
                logger.info(f"🚀 Starting pipeline for {pdf_file}")
                
                # Update the PDF_FILE path in pipeline.py (or pass as argument)
                # For now, we'll copy the file to the expected location
                target_pdf = Path("combinedPdf.pdf")
                
                # Backup existing file if it exists
                if target_pdf.exists():
                    backup_name = f"combinedPdf_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    target_pdf.rename(backup_name)
                    logger.info(f"📋 Backed up existing PDF as {backup_name}")
                
                # Copy the email attachment to the expected location
                import shutil
                shutil.copy2(pdf_file, target_pdf)
                logger.info(f"📄 Copied {pdf_file} to {target_pdf}")
                
                # Run the pipeline
                result = subprocess.run(['python', 'pipeline.py'], 
                                      capture_output=True, text=True)
                
                if result.returncode == 0:
                    logger.info("✅ Pipeline completed successfully")
                    
                    # Run the textract results processing
                    result2 = subprocess.run(['python', 'process_textract_results.py'], 
                                           capture_output=True, text=True)
                    
                    if result2.returncode == 0:
                        logger.info("✅ Textract results processing completed successfully")
                        return True
                    else:
                        logger.error(f"❌ Textract processing failed: {result2.stderr}")
                        return False
                else:
                    logger.error(f"❌ Pipeline failed: {result.stderr}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline trigger error: {e}")
            return False
    
    def mark_as_processed(self, email_id):
        """Mark email as read and optionally move to processed folder"""
        try:
            # Mark as read
            self.imap.store(email_id, '+FLAGS', '\\Seen')
            logger.info(f"📧 Marked email {email_id.decode()} as read")
            
            # Optionally move to processed folder (uncomment if you want this)
            # try:
            #     self.imap.move(email_id, self.config['processed_folder'])
            #     logger.info(f"📁 Moved email to {self.config['processed_folder']}")
            # except:
            #     logger.warning("⚠️ Could not move email to processed folder")
            
        except Exception as e:
            logger.error(f"❌ Error marking email as processed: {e}")
    
    def send_confirmation(self, recipient, original_subject, pdf_count):
        """Send confirmation email"""
        if not self.smtp:
            if not self.connect_smtp():
                return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['email']
            msg['To'] = recipient
            msg['Subject'] = f"Re: {original_subject} - Processing Complete"
            
            body = f"""
Hello,

Your document analysis request has been processed successfully.

Details:
- Number of PDF files processed: {pdf_count}
- Processing completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- Status: ✅ Success

The documents have been analyzed and the results are available in our system.

Best regards,
Document Analysis System
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            self.smtp.send_message(msg)
            logger.info(f"📧 Sent confirmation email to {recipient}")
            
        except Exception as e:
            logger.error(f"❌ Error sending confirmation email: {e}")
    
    def send_error_notification(self, recipient, error_message):
        """Send error notification email"""
        if not self.smtp:
            if not self.connect_smtp():
                return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.config['email']
            msg['To'] = recipient
            msg['Subject'] = "Document Analysis - Error"
            
            body = f"""
Hello,

There was an issue processing your document analysis request.

Error: {error_message}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Please check your email and try again, or contact support.

Best regards,
Document Analysis System
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            self.smtp.send_message(msg)
            logger.info(f"📧 Sent error notification to {recipient}")
            
        except Exception as e:
            logger.error(f"❌ Error sending error notification: {e}")
    
    def cleanup(self):
        """Close connections"""
        try:
            if self.imap:
                self.imap.close()
                self.imap.logout()
            if self.smtp:
                self.smtp.quit()
            logger.info("🔌 Closed email connections")
        except:
            pass

# --------------------------------------------------
# MAIN MONITORING LOOP
# --------------------------------------------------
def monitor_emails():
    """Main email monitoring loop"""
    logger.info("🚀 Starting email monitoring service")
    
    # Validate configuration
    if not EMAIL_CONFIG['email'] or not EMAIL_CONFIG['password']:
        logger.error("❌ Email credentials not configured. Please update EMAIL_CONFIG.")
        return
    
    processor = EmailProcessor(EMAIL_CONFIG)
    
    try:
        while True:
            logger.info("🔍 Checking for new emails...")
            
            # Connect to IMAP
            if not processor.connect_imap():
                logger.error("❌ Failed to connect to email server. Retrying in 5 minutes...")
                time.sleep(300)  # Wait 5 minutes before retrying
                continue
            
            # Search for emails
            email_ids = processor.search_emails()
            
            if email_ids:
                logger.info(f"📧 Found {len(email_ids)} emails to process")
                
                for email_id in email_ids:
                    success = processor.process_email(email_id)
                    if success:
                        logger.info(f"✅ Successfully processed email {email_id.decode()}")
                    else:
                        logger.error(f"❌ Failed to process email {email_id.decode()}")
            else:
                logger.info("📭 No new emails found")
            
            # Close IMAP connection
            processor.cleanup()
            
            # Wait before next check
            logger.info(f"⏰ Waiting {EMAIL_CONFIG['check_interval']} seconds before next check...")
            time.sleep(EMAIL_CONFIG['check_interval'])
            
    except KeyboardInterrupt:
        logger.info("🛑 Email monitoring stopped by user")
    except Exception as e:
        logger.error(f"❌ Email monitoring error: {e}")
    finally:
        processor.cleanup()

# --------------------------------------------------
# CONFIGURATION HELPER
# --------------------------------------------------
def setup_email_config():
    """Helper function to set up email configuration"""
    print("📧 Email Configuration Setup")
    print("=" * 40)
    
    EMAIL_CONFIG['email'] = input("Enter your email address: ")
    EMAIL_CONFIG['password'] = input("Enter your email password (or app password): ")
    
    provider = input("Email provider (gmail/outlook/other): ").lower()
    
    if provider == 'gmail':
        EMAIL_CONFIG['imap_server'] = 'imap.gmail.com'
        EMAIL_CONFIG['smtp_server'] = 'smtp.gmail.com'
    elif provider == 'outlook':
        EMAIL_CONFIG['imap_server'] = 'outlook.office365.com'
        EMAIL_CONFIG['smtp_server'] = 'smtp.office365.com'
    else:
        EMAIL_CONFIG['imap_server'] = input("Enter IMAP server: ")
        EMAIL_CONFIG['smtp_server'] = input("Enter SMTP server: ")
    
    interval = input(f"Check interval in seconds (default {EMAIL_CONFIG['check_interval']}): ")
    if interval:
        EMAIL_CONFIG['check_interval'] = int(interval)
    
    print("\n✅ Configuration updated!")
    print("Note: For Gmail, you may need to use an App Password instead of your regular password.")
    print("Enable 2FA and generate an App Password at: https://myaccount.google.com/apppasswords")

# --------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------
def main():
    """Main execution function"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'setup':
        setup_email_config()
        return
    
    if not EMAIL_CONFIG['email'] or not EMAIL_CONFIG['password']:
        print("❌ Email not configured. Run 'python email_trigger.py setup' first.")
        return
    
    monitor_emails()

if __name__ == "__main__":
    main()