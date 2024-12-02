import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os
import time
from dotenv import load_dotenv

load_dotenv()
# Email alert function with file attachments
def send_email_alert_with_attachment(subject, body, recipient_email, attachment_paths):
    # sender_email = os.getenv("SENDER_EMAIL")

    sender_email = os.environ["SENDER_EMAIL"]
    password = os.environ["SENDER_PASS"]

    #Instanvi chatbot1

    try:
        # Create the email message
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Attach files
        for path in attachment_paths:
            with open(path, 'rb') as file:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename={os.path.basename(path)}'
                )
                msg.attach(part)

        # Send the email
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Replace with your SMTP server and port
        server.starttls()
        server.login(sender_email, password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()

        print(f"Email sent to {recipient_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")


# Function to check anomalies in multiple CSVs
def check_anomalies_and_notify():

    
    # Path to the folder containing pre-existing CSV files
    current_wd = os.getcwd()
    files_path = f"{current_wd}/files"
    sheet_folder_path = f"{files_path}/sheets/"

    # Load the data from CSV files
    vehicule_df = pd.read_csv(f'{sheet_folder_path}vehicule.csv')
    agent_df = pd.read_csv(f'{sheet_folder_path}agent.csv')
    intervention_df = pd.read_csv(f'{sheet_folder_path}intervention.csv')

    summary = []
    attachments = []

    # Vehicule anomalies
    important_columns_vehicule = ['codevehicule', 'nom', 'fabricant', 'immat']
    missing_values_vehicule = vehicule_df[vehicule_df[important_columns_vehicule].isnull().any(axis=1)]
    if not missing_values_vehicule.empty:
        anomaly_path = 'missing_values_vehicule.csv'
        missing_values_vehicule.to_csv(f"{files_path}/anomalies/{anomaly_path}", index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Missing values in 'vehicule.csv' (see {anomaly_path}).")

    duplicate_vehicles = vehicule_df[vehicule_df.duplicated(subset=['codevehicule'], keep=False)]
    if not duplicate_vehicles.empty:
        anomaly_path = 'duplicate_vehicles.csv'
        duplicate_vehicles.to_csv(f"{files_path}/anomalies/{anomaly_path}", index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Duplicate records in 'vehicule.csv' (see {anomaly_path}).")

    outlier_vidange = vehicule_df[vehicule_df['vidange'] > 100000]
    if not outlier_vidange.empty:
        anomaly_path = 'outlier_vidange.csv'
        outlier_vidange.to_csv(f"{files_path}/anomalies/{anomaly_path}", index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Outliers in 'vidange' column of 'vehicule.csv' (see {anomaly_path}).")

    # Agent anomalies
    important_columns_agent = ['codeagent', 'nom', 'prenom', 'email']
    missing_values_agent = agent_df[agent_df[important_columns_agent].isnull().any(axis=1)]
    if not missing_values_agent.empty:
        anomaly_path = 'missing_values_agent.csv'
        missing_values_agent.to_csv(f"{files_path}/anomalies/{anomaly_path}", index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Missing values in 'agent.csv' (see {anomaly_path}).")

    duplicate_agents = agent_df[agent_df.duplicated(subset=['codeagent'], keep=False)]
    if not duplicate_agents.empty:
        anomaly_path = 'duplicate_agents.csv'
        duplicate_agents.to_csv(f"{files_path}/anomalies/{anomaly_path}", index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Duplicate records in 'agent.csv' (see {anomaly_path}).")

    # Intervention anomalies
    important_columns_intervention = ['codeintervention', 'niveau', 'lieu', 'probleme']
    missing_values_intervention = intervention_df[intervention_df[important_columns_intervention].isnull().any(axis=1)]
    if not missing_values_intervention.empty:
        anomaly_path = 'missing_values_intervention.csv'
        missing_values_intervention.to_csv(anomaly_path, index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Missing values in 'intervention.csv' (see {anomaly_path}).")

    duplicate_interventions = intervention_df[intervention_df.duplicated(subset=['codeintervention'], keep=False)]
    if not duplicate_interventions.empty:
        anomaly_path = 'duplicate_interventions.csv'
        duplicate_interventions.to_csv(f"{files_path}/anomalies/{anomaly_path}", index=False)
        attachments.append(f"{files_path}/anomalies/{anomaly_path}")
        summary.append(f"Duplicate records in 'intervention.csv' (see {anomaly_path}).")

    # Send email if anomalies detected
    if summary:
        report = "Summary of Anomalies Detected:\n\n" + "\n".join(summary)
        print(report)  # For debugging
        send_email_alert_with_attachment(
            "Anomaly Report Summary",
            report,
            "delmaschris7@gmail.com",
            attachments
        )
    else:
        print("No anomalies detected.")


# Schedule the function to run every hour
def run_periodically(interval_seconds):
    while True:
        print("Checking for anomalies...")
        check_anomalies_and_notify()
        print(f"Waiting for {interval_seconds} seconds before the next check...")
        time.sleep(interval_seconds)


# Run the anomaly checker every hour (3600 seconds)
# run_periodically(3600)
