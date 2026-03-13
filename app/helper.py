import pandas as pd
from app.variables import rename_columns, behavior_to_drop_columns

def name_and_card_reference_transformers(main_data_dump):
    name_reference_df =  pd.read_excel(main_data_dump / 'Name References.xlsx', sheet_name = 'DUMP')
    ccp_name_reference_df =  pd.read_excel(main_data_dump / 'Name References.xlsx', sheet_name = 'CCP Name')
    card_type_refereces_for_vg_toolkit_df = pd.read_excel(main_data_dump / 'Card References.xlsx', sheet_name = 'Sheet1', usecols = ['PAT Card List', 'Referrence'])

    team_leader_reference_df = name_reference_df[['Team Leader', 'Team Leader New Name']].copy().dropna()
    monitored_by_reference_df = name_reference_df[['Monitored By', 'Monitored By New Name']].copy().dropna()
    manager_reference_df = name_reference_df[['Manager', 'Manager New Name']].copy().dropna()

    team_leader_reference_df = dict(zip(team_leader_reference_df['Team Leader'], team_leader_reference_df['Team Leader New Name']))
    monitored_by_reference_df = dict(zip(monitored_by_reference_df['Monitored By'], monitored_by_reference_df['Monitored By New Name']))
    manager_reference_df = dict(zip(manager_reference_df['Manager'], manager_reference_df['Manager New Name']))
    ccp_name_reference_df = dict(zip(ccp_name_reference_df['Employee ID'], ccp_name_reference_df['CCP Name']))
    card_type_refereces_for_vg_toolkit_df = dict(zip(card_type_refereces_for_vg_toolkit_df['PAT Card List'], card_type_refereces_for_vg_toolkit_df['Referrence']))
    
    return {
        "team_leader_reference_df" : team_leader_reference_df,
        "monitored_by_reference_df" : monitored_by_reference_df,
        "manager_reference_df" : manager_reference_df,
        "ccp_name_reference_df" : ccp_name_reference_df,
        "card_type_refereces_for_vg_toolkit_df" : card_type_refereces_for_vg_toolkit_df,
    }

def add_months(r):
    r = str(r)
    if r == 'OJT':
        return r
    if "months" not in r:
        r = r.strip()
        return r + " months"
    return r

def behavior_transformation_sorter(r):
    if r == 'Warm Welcome' or r == 'Human Connection':
        return 1
    if r == 'Acknowledge Emotion' or r == 'Recognize and Promote Relationship' or r == 'Share a Relevant Offer or Message' or r == 'Emphasize Resolution/Recap Actions':
        return 3
    if r == 'Take Ownership' or r == 'Discover Solutions':
        return 2
    if r == 'Show Appreciation':
        return 4

def behavior_transformation(r):
    if r == 'Warm Welcome' or r == 'Human Connection':
        return 'Open with Connection'
    if r == 'Acknowledge Emotion' or r == 'Recognize and Promote Relationship' or r == 'Share a Relevant Offer or Message' or r == 'Emphasize Resolution/Recap Actions':
        return 'Deepen Relationship'
    if r == 'Take Ownership' or r == 'Discover Solutions':
        return 'Discover Solution'
    if r == 'Show Appreciation':
        return 'Close with Confidence'

def sub_behavior_copy_transformation(r):
    if r == 'Warm Welcome':
        return 1
    if r == 'Human Connection':
        return 2
    if r == 'Acknowledge Emotion':
        return 3
    if r == 'Take Ownership':
        return 4
    if r == 'Discover Solutions':
        return 5
    if r == 'Recognize and Promote Relationship':
        return 6
    if r == 'Share a Relevant Offer or Message':
        return 7
    if r == 'Emphasize Resolution/Recap Actions':
        return 8
    if r == 'Show Appreciation':
        return 9
    
def call_segment_transformation(r):
    if r == 'OPEN - Call Segment In Seconds':
        return 'Open With Connection'
    if r == 'SOLVE - Call Segment In Seconds':
        return 'Commit To Solve'
    if r == 'DEEPEN RELATIONSHIP - Call Segment In Seconds':
        return 'Deepen Relationship'
    if r == 'CLOSE WITH CONFIDENCE - Call Segment In Seconds':
        return 'Close with Confidence'

def call_segment_transformation_sorter(r):
    if r == 'Open With Connection':
        return 1
    if r == 'Commit To Solve':
        return 2
    if r == 'Deepen Relationship':
        return 3
    if r == 'Close with Confidence':
        return 4
    
def vg_main_components_transform(r):
    if r == 'Explore CVP and Identify Clues' or r == 'Listen and Discover':
        return 'Explore and Discover'
    if r == 'Connect a Relevant Offer' or r == 'Transition with a Relevant Statement':
        return 'Connect & Transition'
    if r == 'Communicate Features of the Offer' or r == 'Personalize Value and Benefit':
        return 'Communicate & Personalize'
    if r == 'Process the Offer Compliantly':
        return 'Fullfil with Care'

def vg_main_components_sorter(r):
    if r == 'Explore CVP and Identify Clues' or r == 'Listen and Discover':
        return 1
    if r == 'Connect a Relevant Offer' or r == 'Transition with a Relevant Statement':
        return 2
    if r == 'Communicate Features of the Offer' or r == 'Personalize Value and Benefit':
        return 3
    if r == 'Process the Offer Compliantly':
        return 4
    
def get_value(x):
        if x == 'MET' or x == 'Met W/Opportunities' or x == 'Met' or x == 'Met with Opportunities':
            return 1
        return 0

def vg_sub_components_sorter(r):
    if r == 'Explore CVP and Identify Clues': 
        return 1
    if r == 'Listen and Discover': 
        return 2
    if r == 'Connect a Relevant Offer': 
        return 3
    if r == 'Transition with a Relevant Statement': 
        return 4
    if r == 'Communicate Features of the Offer': 
        return 5
    if r == 'Personalize Value and Benefit':  
        return 6
    if r == 'Process the Offer Compliantly': 
        return 7
    
def accept_rate(r):
    if r == 'Accepted':
        return 1
    return 0

def decline_rate(r):
    if r == 'Declined':
        return 1
    return 0 

def not_applicable_rate(r):
    if r == 'Not Applicable':
        return 1
    return 0

def vg_coaches_transform(r, coaches):
    if r in coaches:
        return 1
    return 0

def tenure_sorter_transform(r):
    if r == '12+ months':
        return 1
    if r == '6-12 months':
        return 2
    if r == "0-6 months":
        return 3
    return 4

def directory_checker(dir):
    if not dir.exists():
        return False
    return True

def remove_parentheses_content(s):
    if isinstance(s, str):
        if '(' in s and ')' in s:
            before = s[:s.find('(')]
            after = s[s.find(')')+1:]
            return (before + after).strip()
        return s.strip()
    return s 

def raw_data_consolidator(folder_directory):
    dfs = []
    
    for file in folder_directory.glob("*.xlsx"):
        df = pd.read_excel(file)
        df = df.applymap(lambda x: x.replace('\n', '') if isinstance(x, str) else x)
        df.columns = df.columns.str.replace('\n', '', regex=False)
        df = df.rename(columns = rename_columns)
        df['CCP Name'] = df['CCP Name'].apply(remove_parentheses_content)
        df['Team Leader'] = df['Team Leader'].apply(remove_parentheses_content)
        df['Manager'] = df['Manager'].apply(remove_parentheses_content)
        df["CHT (sec/s)"] = df["CHT (sec/s)"].astype(str).str.replace("Secs", "").astype(float)
        df["OPEN - Call Segment In Seconds"] = df["OPEN - Call Segment In Seconds"].astype(str).str.replace("Secs", "").astype(float)
        df["SOLVE - Call Segment In Seconds"] = df["SOLVE - Call Segment In Seconds"].astype(str).str.replace("Secs", "").astype(float)
        df["DEEPEN RELATIONSHIP - Call Segment In Seconds"] = df["DEEPEN RELATIONSHIP - Call Segment In Seconds"].astype(str).str.replace("Secs", "").astype(float)
        df["CLOSE WITH CONFIDENCE - Call Segment In Seconds"] = df["CLOSE WITH CONFIDENCE - Call Segment In Seconds"].astype(str).str.replace("Secs", "").astype(float)
        df["Total Time of Call - Call Segment In Seconds"] = df["Total Time of Call - Call Segment In Seconds"].astype(str).str.replace("Secs", "").astype(float)
        df["Waste (Actual CHT - Total Time Spent)"] = df["Waste (Actual CHT - Total Time Spent)"].astype(str).str.replace("Secs", "").astype(float)
        df['Site'] = df['Site'].replace({
            "MANILA":"QUEZON CITY",
            "ILOILO":"ILOILO CITY",
        })
        dfs.append(df)

    consolidated_df = pd.concat(dfs)

    consolidated_df = consolidated_df[consolidated_df['Tenure'] != 'Support']

    consolidated_df['Tenure'] = consolidated_df['Tenure'].apply(add_months)

    return consolidated_df

def attendance_consolidator(directory):
    return pd.concat([
        attendance_transformer(file) for file in directory.glob('*.csv')
    ])

def attendance_transformer(file):
    columns_to_include = [
        "EID",
        "CCP Name",
        "Site",
        "FIG",
        "Manager",
        "Team Lead",
        "Tenure",
        "Month",
    ]
    
    df = pd.read_csv(file, encoding = 'latin-1')

    df['Month'] = str(file).split("\\")[-1].split('.')[0]
    
    df = df.drop(columns = ['CID', 'CCP Status'])

    for col in df.columns:
        converted = pd.to_datetime(col, errors='coerce')
        if not pd.isna(converted):
            if col not in columns_to_include:
                columns_to_include.append(col)
    
    melted_df = pd.melt(df, var_name = 'Week Start', value_name = 'Present', id_vars=[
        "EID",
        "CCP Name",
        "Site",
        "FIG",
        "Manager",
        "Team Lead",
        "Tenure",
        "Month"
    ])

    melted_df['Week Start'] = pd.to_datetime(melted_df['Week Start'])

    melted_df['FIG'] = melted_df['FIG'].replace({"HVCM Hilton Servicing":"Hilton"})
    
    return melted_df

def behavior_df_transform(df):
    behavior_df = df.drop(columns = behavior_to_drop_columns).copy()
    behavior_df = pd.melt(behavior_df, id_vars=[
        "Employee ID",
        "CCP Name",
        "Team Leader",
        "TL",
        "Manager",
        "Monitored By Name",
        "Tenure",
        "FIG",
        "Site",
        "Interaction ID",
        "CHT (sec/s)",
        "Card Type",
        "OPEN - Call Segment In Seconds",
        "SOLVE - Call Segment In Seconds",
        "DEEPEN RELATIONSHIP - Call Segment In Seconds",
        "CLOSE WITH CONFIDENCE - Call Segment In Seconds",
        "Caller Type",
        "Call Reason",
        "Monitoring Date",
        "Monitor Date",
        "Monitored By",
        "Monitered Employee Id",
        "TL=Monitored By",
        "FIG NEW",
        "Month",
        "Week Start ",
        "Ref",
        'CCP_1', 
        'Acknowledged'
    ], var_name="Sub Behavior", value_name="Sub Behavior Value")
    behavior_df['Sub Behavior'] = behavior_df['Sub Behavior'].replace({
        "Human Connection (Listen)":"Human Connection",
        "Acknowledge Emotion (Acknowledge)":"Acknowledge Emotion",
        "Discover Solutions (Explore)":"Discover Solutions",
        "Share a Relevant Offer or Message (Recommend)":"Share a Relevant Offer or Message",
    })
    
    behavior_df['Sub Behavior - Copy'] = behavior_df['Sub Behavior'].apply(sub_behavior_copy_transformation)
    behavior_df['Behavior'] = behavior_df['Sub Behavior'].apply(behavior_transformation)
    behavior_df['Behavior - Copy'] = behavior_df['Sub Behavior'].apply(behavior_transformation_sorter)
    return behavior_df