import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
import time

def send_mail(subject, body, gmail_user, gmail_pass):
    """Send notification email using Gmail App Password"""
    recipients = [
        "manas.barnwal@kazam.in",
        "vishnu.vardhan@kazam.in",
        "harshit.jain@kazam.in",
        "tejas.saxena@kazam.in,
        "sukriti@kazam.in"
    ]

    if not gmail_user or not gmail_pass:
        print("⚠️ Missing Gmail credentials in environment variables.")
        return

    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, recipients, msg.as_string())
        print(f"📧 Email sent successfully to: {', '.join(recipients)}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
