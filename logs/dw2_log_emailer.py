import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime

# Function to scan logs for WARNING messages
def scan_logs_for_warnings(log_folder):
    today = datetime.date.today().strftime('%Y-%m-%d')
    warning_messages = []
    for filename in os.listdir(log_folder):
        if filename.endswith('.log'):
            with open(os.path.join(log_folder, filename), 'r') as file:
                for line in file:
                    if 'WARNING' in line:                               # look for warning messages only
                        if today in line:                               # get data from today only
                            if "Atribute" not in line:                  #This removes false positives
                                warning_messages.append(f"{filename}: {line}")
    return warning_messages

# Function to send email
def send_email(sender_email, sender_password, recipient_email, subject, body):
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = message.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        print("Email sent successfully")
    except Exception as e:
        print("Failed to send email:", e)

# Main function
def main():
    os.chdir("./etl2code/logs")
    log_folder  = os.getcwd()
    warnings = scan_logs_for_warnings(log_folder)
    
    if warnings:
        subject = 'WARNING messages in logs found today'
        body = '\n'.join(warnings)
        sender_email = 'kinetixopensprocessing@gmail.com'  # Enter your email here
        sender_password = 'ttljtrsnsqlhmnrz'      # Enter your email password here
        recipient_email = 'DART@kinetixhr.com'  # Enter recipient email here
        send_email(sender_email, sender_password, recipient_email, subject, body)
    else:
        print("No WARNING messages found in logs today!")

if __name__ == "__main__":
    main()
