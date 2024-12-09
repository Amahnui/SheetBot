import pandas as pd
import streamlit as st
import re
import os
import threading
from utils.anomaly_checker import run_periodically
import openai, os
from dotenv import load_dotenv
from langchain_experimental.agents.agent_toolkits import create_csv_agent
from langchain_openai import ChatOpenAI


load_dotenv()

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY") 

df = pd.read_csv('combined_data.csv')

# Function to load a CSV file into a DataFrame
def load_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading CSV file: {e}")
        return None

# Function to save the DataFrame to a CSV file
def save_csv(df, file_path, trigger):
    try:
        df.to_csv(file_path, index=False)
        if trigger == "update":
            st.success("File updated successfully.")
        elif trigger == "add":
            st.success("Record(s) added successfully.")
        elif trigger == "delete":
            st.success("Records deleted successfully.")
        else:
            st.success("Changes made to File were successful")
    except Exception as e:
        st.error(f"Error saving CSV file: {e}")

# Function to add a record
def add_record(new_data, df, file_path):
    try:
        # Ensure the input is a dictionary
        if not isinstance(new_data, dict):
            return "Error: The new record data must be a dictionary."

        # Validate that all specified columns exist in the DataFrame
        missing_columns = [col for col in new_data.keys() if col not in df.columns]
        if missing_columns:
            return f"Error: The following columns are missing in the dataset: {', '.join(missing_columns)}"

        # Create a new row as a DataFrame
        new_row = pd.DataFrame([new_data], columns=df.columns)

        # Concatenate the new row with the original DataFrame
        df = pd.concat([df, new_row], ignore_index=True)

        # Save back to the file
        df.to_csv(file_path, index=False)
        # return "Record added successfully."
        return new_row
    except Exception as e:
        return f"Error adding record: {e}"



# Function to update records based on a condition
def update_record(condition, update_values, df, file_path):
    try:
        for col, value in update_values.items():
            df.loc[condition, col] = value
        save_csv(df, file_path, "update")
        # return "Records updated successfully."
        return df
    except Exception as e:
        st.error(f"Error updating records: {e}")
        return "Error updating records."

# Function to delete records based on a condition
def delete_record(condition, df, file_path):
    try:
        df = df[~condition]  # Filter out rows that match the condition
        # deleted_df = df[condition]
        save_csv(df, file_path, "delete")
        return "Deleted Successfully"
    except Exception as e:
        st.error(f"Error deleting records: {e}")
        return "Error deleting records."


# Function to handle various user queries
def handle_instruction(instruction_org, df, file_path):
    try:

        date_keywords = {
            "after": ["after", "après"],
            "before": ["before", "avant"],
            "on": ["on", "le"],
        }

        keywords = {
            "add": ["add", "ajouter"],
            "update": ["update", "edit", "set", "modifier", "mettre"],
            "delete": ["delete", "remove", "supprimer", "retirer"],
            "find": ["find", "trouver", "rechercher", "chercher", "cherche"],
            "where": ["where", "où", "ou"],
            "is": ["is", "est"],
            "greater than": ["greater than", "supérieur à", "plus que"],
            "less than": ["less than", "inférieur à", "moins que"],
            "equals": ["equals", "égal à"],
            "contains": ["contains", "contient", "à"],
            "is": ["is", "est"],
            "of": ["of", "de"],
            "before": ["before", "avant"],
            "after": ["after", "après"]
        }
        instruction = instruction_org.lower()
        # Handle "add" queries
        # if "add" in instruction.lower():
        print("Step 1")
        if any(keyword in instruction for keyword in keywords["add"]):
            # Assuming a simple format: "Add a record where column1 is value1, column2 is value2, ..."
            # pattern = re.findall(r"(\w+)\s*is\s*([\w\s]+)", instruction, re.IGNORECASE)
            pattern = re.findall(r"(\w+)\s*(?:is|est)\s*([\w\s]+)", instruction, re.IGNORECASE)
            if not pattern:
                return "Could not parse the addition instruction. Please follow the format: 'Add a record where column1 is value1, column2 is value2, ...'"

            new_data = {}
            for col, value in pattern:
                if col in df.columns:
                    new_data[col] = value

            if not new_data:
                return "No valid columns found for the new record."
            print(new_data)
            result = add_record(new_data, df, file_path)
            st.success("Record Added successfully.")
            return result

        # Handle "update" queries
        # if ("update" in instruction.lower()) or ("edit" in instruction.lower()) or ("set" in instruction.lower()):
        #     # Match "Update column to value where condition"
        #     condition_match = re.search(
        #         r"update\s+(\w+)\s+to\s+([\w\s\d.]+)\s+where\s+(.+)",
        #         instruction,
        #         re.IGNORECASE,
        #     )
        print("Step 2")
        if any(keyword in instruction for keyword in keywords["update"]):
            condition_match = re.search(
                r"(?:update|modifier)\s+(\w+)\s+(?:to|à)\s+([\w\s\d.]+)\s+(?:where|où)\s+(.+)",
                # r"(update|modifier)\s+(\w+)\s+(to|à)\s+([\w\s\d.]+)\s+(where|où)\s+(.+)",
                instruction,
                re.IGNORECASE,
            )

            if not condition_match:
                return "Could not parse the update instruction. Please follow the format: 'Update column to value where condition'."
            column_to_update = condition_match.group(1).strip()
            new_value = condition_match.group(2).strip()
            condition = condition_match.group(3).strip()
            print(condition_match.groups())
            # Ensure column exists
            if column_to_update not in df.columns:
                return f"Column '{column_to_update}' not found in the DataFrame."

            # Split conditions by "or"
            or_conditions = [cond.strip() for cond in condition.split(" or ")]

            # Initialize the complete query
            complete_query = None

            for or_condition in or_conditions:
                # Parse each individual condition (supports AND within OR groups)
                and_conditions = [cond.strip() for cond in or_condition.split(" and ")]
                sub_query = None

                for and_condition in and_conditions:
                    match = re.search(
                        r"(\w+)\s+(is|equals|contains|greater than|less than)\s+([\w\s\d.]+)",
                        and_condition,
                        re.IGNORECASE,
                    )
                    if not match:
                        return f"Could not parse the condition: {and_condition}"

                    col, operator, value = match.groups()
                    col, value = col.strip(), value.strip()

                    if col not in df.columns:
                        return f"Column '{col}' not found in the DataFrame."

                    # Handle numeric or string conditions
                    if pd.api.types.is_numeric_dtype(df[col]):
                        if operator in ["greater than", "less than"]:
                            value = float(value) if value.replace(".", "", 1).isdigit() else value
                            condition_clause = df[col] > value if operator == "greater than" else df[col] < value
                        else:
                            value = float(value) if value.replace(".", "", 1).isdigit() else value
                            condition_clause = df[col] == value
                    else:
                        df[col] = df[col].astype(str)  # Ensure string comparison
                        if operator in ["is", "equals"]:
                            condition_clause = df[col].str.lower() == value.lower()
                        elif operator == "contains":
                            condition_clause = df[col].str.contains(value, case=False, na=False)
                        else:
                            return f"Unsupported operator: {operator}"

                    sub_query = condition_clause if sub_query is None else sub_query & condition_clause

                # Combine OR conditions
                complete_query = sub_query if complete_query is None else complete_query | sub_query

            if complete_query is None:
                return "Could not construct the condition. Please check your syntax."

            # Ensure new_value is compatible with the target column
            if pd.api.types.is_numeric_dtype(df[column_to_update]):
                new_value = float(new_value) if new_value.replace(".", "", 1).isdigit() else new_value
            else:
                new_value = str(new_value)

            # Perform the update
            df.loc[complete_query, column_to_update] = new_value

            # Save the updated DataFrame to the file
            df.to_csv(file_path, index=False)
            # return f"Records updated successfully in column '{column_to_update}'."
            st.success("File updated successfully.")
            return df

        # Handle "delete" queries
        # if (("delete" in instruction.lower())) or ("remove" in instruction.lower()):
        #     # Match "Delete records where column operator value"
        #     condition_match = re.search(
        #         r"(\w+)\s+(greater than|less than|equals|is|contains)\s+([\w\s\d.]+)",
        #         # r"delete data where\s+(\w+)\s+(greater than|less than|equals|is|contains)\s+([\w\s\d.]+)",
        #         # r"(delete|remove)\s+[\w\s]+\s+where\s+(greater than|less than|equals|is|contains|has)\s+([\w\s\d.]+)",
        #         instruction,
        #         re.IGNORECASE,
        #     )
        print("Step 3")
        if any(keyword in instruction for keyword in keywords["delete"]):
            condition_match = re.search(
                # r"(\w+)\s+(?:greater than|supérieur à|plus que|moin que|less than|inférieur à|equals|égal à|is|est|contains|contient|à)\s+([\w\s\d.]+)",
                r"(\w+)\s+(greater than|supérieur à|plus que|moin que|less than|inférieur à|equals|égal à|is|est|contains|contient|à)\s+([\w\s\d.]+)",
                instruction,
                re.IGNORECASE,
            )
            if not condition_match:
                return "Could not parse the delete instruction. Please follow format `delete(or 'remove') records where [your condition values]` or specify the condition using 'greater than', 'less than', 'equals', or 'contains'."
            
            col = condition_match.group(1).strip()
            operator = condition_match.group(2).lower()
            value = condition_match.group(3).strip()
            print(condition_match.groups())
            # Ensure column exists
            if col not in df.columns:
                return f"Column '{col}' not found in the DataFrame."

            # Identify column data type and condition
            if operator in ["greater than", "supérieur à", "plus que", "less than", "inférieur à", "moins que", "equals", "equals", "égal à"]:
                # Check if column can be coerced to numeric
                if pd.api.types.is_numeric_dtype(df[col]) or df[col].apply(lambda x: str(x).replace('.', '', 1).isdigit()).all():
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    value = float(value) if value.replace(".", "", 1).isdigit() else value

                    # Create numeric condition
                    if operator in ["greater than", "supérieur à", "plus que"]:
                        condition = df[col] > value
                    elif operator in ["less than", "inférieur à", "moins que"]:
                        condition = df[col] < value
                    else:  # "equals" or "equal to"
                        condition = df[col] == value
                else:
                    return f"Column '{col}' does not support numerical operations."
            elif operator in ["contains","has" "contient", "à"]:
                # Ensure the column is string-compatible
                if not pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].astype(str)
                condition = df[col].str.contains(value, case=False, na=False)
            elif operator in ["is"]:
                if not pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].astype(str)
                condition = df[col].fillna('').str.lower() == value.lower()
            else:
                return "Unsupported operator. Use 'greater than', 'less than', 'equals', or 'contains'."

            # Perform deletion
            result = delete_record(condition, df, file_path)

            return result


        # Handle "how many" queries and other operations...
        # (Add the existing code from your previous `handle_instruction` function here.)

        print("Step 4")

        llm = ChatOpenAI(temperature=0.5)

        # agent_executer = create_csv_agent(llm,"combined_data.csv", verbose=True, allow_dangerous_code=True)
        
        agent_executer = create_csv_agent(llm, file_path, verbose=True, allow_dangerous_code=True)
        # result = df[df["fabricant"].str.lower() == "toyota"]

        # response = agent_executer.invoke("How many records are from agent's file?")
        response = agent_executer.invoke(instruction_org)
        print(response)
        return response["output"]

    except Exception as e:
        st.error(f"Error handling instruction: {e}")
        return f"Error handling instruction: {e}"

def start_periodic_task(interval):
    """Starts the anomaly check task in a separate thread."""
    periodic_thread = threading.Thread(target=run_periodically, args=(interval,))
    periodic_thread.daemon = True  # Ensures the thread exits when the main program exits
    periodic_thread.start()
    print(f"Background task started: running every {interval} seconds.")


# Main function for the Streamlit app

def main():
    # Set up page layout
    st.set_page_config(page_title="Chatbot", layout="wide")

    st.markdown(
        """
        <style>
        :root {
            --input-text-color-light: #232324; /* Dark color for light mode */
            --input-text-color-dark: white;  /* Light color for dark mode */
        }


        .chat-message {
            margin: 10px 0;
            padding: 10px;
            border-radius: 8px;
            max-width: 60%;
        }
        .user-message {
            //align-self: flex-start;
            background-color: #0084ff;
            color: white;
            //margin-left: 300px;
            margin-left: 40%;
        }
        .bot-message {
            //align-self: flex-end;
            background-color: #e4e6eb;
            color: #232324;
            margin-right: 10px;
        }


        .stForm {
            position: fixed;
            bottom: 5%; /* Adjusted to make it more balanced */
            left: 50%; /* Center the form */
            transform: translateX(-50%); /* Center alignment */
            width: 70%; /* Responsive width */
            max-width: 800px; /* Optional: limit the max width */
            border: 0;
            z-index: 1000; /* Make sure it appears on top */
            # background-color: #f7f7f7;
            background-color: #a8a7a7;
            border-radius: 8px; /* Optional: rounded corners */
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2); /* Optional: add shadow */
        }

        @media screen and (max-width: 1024px) {
            .stForm {
                width: 90%; /* Adjust width for tablets and smaller desktop views */
                left: 50%;
            }
        }

        @media screen and (max-width: 768px) {
            .stForm {
                width: 100%; /* Full width for mobile devices */
                left: 0; /* Align to the left side */
                transform: translateX(0); /* No centering */
            }
        }

        @media (prefers-color-scheme: light) {
            :root {
                --input-text-color: var(--input-text-color-light);
            }
        }

        @media (prefers-color-scheme: dark) {
            :root {
                --input-text-color: var(--input-text-color-dark);
            }
        }
        
        input[class]{
            font-size:80%;
            # color: white;
            color: var(--input-text-color);
            # background-color: #deddd9;
            
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Sidebar for file selection
    current_wd = os.getcwd()
    sheet_folder_path = f"{current_wd}/files/sheets"
    csv_files = [f for f in os.listdir(sheet_folder_path) if f.endswith('.csv')]
    csv_files.sort()

    # st.sidebar.header("File Selection")
    # selected_file = st.sidebar.selectbox("Select a CSV file:", csv_files)
    df = None
    file_path = None
    # if selected_file:
    if True:
        # file_path = f'{sheet_folder_path}/{selected_file}'
        file_path = f'{sheet_folder_path}/combined_data.csv'
        df = load_csv(file_path)

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    # Display chat history
    with st.container(border=True):
        st.markdown('<div class="chat-container">', unsafe_allow_html=True)
        for msg in st.session_state["messages"]:
            if msg["type"] == "text":
                sender_class = "bot-message" if msg["sender"] == "bot" else "user-message"
                st.markdown(
                    f'<div class="chat-message {sender_class}">{msg["content"]}</div>',
                    unsafe_allow_html=True,
                )
            elif msg["type"] == "dataframe":
                st.write("### Response:")
                st.dataframe(msg["content"], use_container_width=True)
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Input box at the bottom
    with st.form("chat_input_form", clear_on_submit=True):
        user_input = st.text_input("Type your query:", "", key="chat_input", label_visibility="hidden", placeholder="Type your query")
        submitted = st.form_submit_button("Send")
    # Handle user input and bot response
    if submitted and user_input.strip():
        # Add user message to chat history
        st.session_state["messages"].append({"sender": "user", "type": "text", "content": user_input})

        if df is None:
            bot_response = "Please select a file before querying."
            st.session_state["messages"].append({"sender": "bot", "type": "text", "content": bot_response})
        else:
            # Call query handling logic
            result = handle_instruction(user_input, df, file_path)
            if isinstance(result, pd.DataFrame):
                st.session_state["messages"].append({"sender": "bot", "type": "dataframe", "content": result})
            else:
                st.session_state["messages"].append({"sender": "bot", "type": "text", "content": result})

        # Refresh to display the new message
        st.rerun()


if __name__ == "__main__":
    # start_periodic_task(3600)  # Runs every 1 hour
    main()
