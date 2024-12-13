import os
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

# Function to detect numerical anomalies
def detect_numerical_anomalies(df, column):
    model = IsolationForest(contamination=0.05, random_state=42)
    df['scaled'] = StandardScaler().fit_transform(df[[column]])
    df['anomaly'] = model.fit_predict(df[['scaled']])
    return df[df['anomaly'] == -1]

# Generate Agent File Anomaly Report
def analyze_agent_file(file_path):
    agent_df = pd.read_csv(file_path)
    anomalies = {}

    # Duplicate agents
    anomalies['Duplicate Agents'] = agent_df[agent_df.duplicated(subset='codeagent')]

    # Missing critical fields
    anomalies['Missing Critical Fields'] = agent_df[agent_df[['codeagent', 'nom', 'matricule']].isnull().any(axis=1)]

    # Future dates
    agent_df['datenais'] = pd.to_datetime(agent_df['datenais'], errors='coerce')
    agent_df['datecreated'] = pd.to_datetime(agent_df['datecreated'], errors='coerce')
    # anomalies['Future Dates'] = agent_df[agent_df['datenais'] > pd.Timestamp.now()]
    # Check for future dates in both columns
    anomalies['Future Dates'] = agent_df[
        (agent_df['datenais'] > pd.Timestamp.now()) | 
        (agent_df['datecreated'] > pd.Timestamp.now())
    ]

    # Notation anomalies
    # Notation anomalies
    if 'notation' in agent_df.columns and not agent_df['notation'].dropna().empty:
        # Only proceed if 'notation' has valid numeric data
        clean_notation_df = agent_df.dropna(subset=['notation'])
        anomalies['Notation Anomalies'] = detect_numerical_anomalies(clean_notation_df, 'notation')
    else:
        anomalies['Notation Anomalies'] = pd.DataFrame()  # No anomalies if column is missing or empty

    return anomalies

# Generate Vehicle File Anomaly Report
def analyze_vehicle_file(file_path):
    vehicle_df = pd.read_csv(file_path)
    anomalies = {}

    # Duplicate vehicles
    anomalies['Duplicate Vehicles'] = vehicle_df[vehicle_df.duplicated(subset='codevehicule')]

    # Missing critical fields
    anomalies['Missing Critical Fields'] = vehicle_df[vehicle_df[['codevehicule', 'immat', 'nom']].isnull().any(axis=1)]

    # Future dates
    vehicle_df['datecreated'] = pd.to_datetime(vehicle_df['datecreated'], errors='coerce')
    anomalies['Future Dates'] = vehicle_df[vehicle_df['datecreated'] > pd.Timestamp.now()]

    # Price anomalies
    if 'prixpjour' in vehicle_df.columns:
        anomalies['Price Anomalies'] = detect_numerical_anomalies(vehicle_df.dropna(subset=['prixpjour']), 'prixpjour')

    return anomalies

# Compile Report as HTML
def compile_report(agents_anomalies, vehicles_anomalies):
    report = "<html><body><h1>Anomaly Report</h1><br><p>We compiled a list of anomalies found, take a look at the data below</p>"
    report += "<h2>Agent File</h2>"
    for key, df in agents_anomalies.items():
        report += f"<h3><br>{key}</h3>"
        report += df.to_html(index=False) if not df.empty else "<p>No anomalies detected.</p>"

    report += "<h2>Vehicle File</h2>"
    for key, df in vehicles_anomalies.items():
        report += f"<h3>{key}</h3>"
        report += df.to_html(index=False) if not df.empty else "<p>No anomalies detected.</p>"

    report += "</body></html>"
    return report

# Send Email with Report
def send_email(report, recipient_email):

    subject = "Anomaly Detection Report"
    sender_email = os.environ["SENDER_EMAIL"]
    sender_password = os.environ["SENDER_PASS"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = recipient_email

    # Attach report as HTML
    html_part = MIMEText(report, "html")
    msg.attach(html_part)

    # Send email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        print("Email sent successfully!")



# Main Workflow
# if __name__ == "__main__":
def execute():
    current_wd = os.getcwd()
    print(current_wd)
    sheet_folder_path = f"{current_wd}/files/sheets"
    print("Preparing to analyze")
    agent_anomalies = analyze_agent_file(f"{sheet_folder_path}/agent.csv")
    print("Analyzed Agent File")
    vehicle_anomalies = analyze_vehicle_file(f"{sheet_folder_path}/vehicule.csv")
    print("Analyzed Vehicle File")

    # Compile and send the report
    email_report = compile_report(agent_anomalies, vehicle_anomalies)
    print("Compiled Report")
    receiver_email = os.environ["RECEIVER_EMAIL"]
    send_email(email_report, receiver_email)
    print("Email Sent")
