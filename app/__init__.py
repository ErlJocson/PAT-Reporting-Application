from rich import print
from app.vg_toolkit import vg_toolkit_df_transform
from app.vg_monitoring_adherence import coach_and_qa_transform
from app.helper import (
    remove_parentheses_content,
    add_months,
    behavior_transformation,
    behavior_transformation_sorter,
    sub_behavior_copy_transformation,
    call_segment_transformation,
    call_segment_transformation_sorter,
    get_value
)
from app.variables import (
    rename_columns,
    behavior_to_drop_columns,
    segment_to_drop_columns,
    dump_to_drop_columns
)
from app.transformers import name_and_card_reference_transformers
import os
import pandas as pd
import numpy as np
import threading
import time

def start(main_data_dump, output_directory) -> None:
    print("PAT Reporting Automation Initiated ...")

    start_time = time.time()

    folder_directory = main_data_dump / "raw"

    REMOVE_REFERENCE = pd.read_excel(main_data_dump / 'Name References.xlsx', sheet_name = 'REMOVE')

    coach_and_qa_df = pd.read_excel(main_data_dump / 'Name References.xlsx', sheet_name = 'REMOVE', usecols=['Month2', 'Coaches' , 'Position', 'Monitor Target'])

    coach_and_qa_df['ref'] = coach_and_qa_df['Month2'].dt.month_name() + coach_and_qa_df['Month2'].dt.year.astype(str) + coach_and_qa_df['Coaches']
    coach_and_qa_df = coach_and_qa_df.drop(columns = ['Month2', 'Coaches'])

    to_delete = REMOVE_REFERENCE[['Month1', 'Delete']]

    to_delete['Delete'] = to_delete['Month1'] + '-' + to_delete['Delete']

    to_delete = to_delete[['Delete']]

    to_delete['Delete'] = to_delete['Delete'].dropna()

    to_remove_names = REMOVE_REFERENCE[['Month2', 'Coaches']].dropna()

    to_remove_names['Coaches'] = to_remove_names['Month2'].dt.month_name() + '-' + to_remove_names['Coaches']
    to_remove_names = to_remove_names[['Coaches']]

    coaches = to_remove_names['Coaches'].dropna().values
    to_delete = to_delete['Delete'].dropna().values

    month_references = pd.read_excel(main_data_dump / 'Month References.xlsx')
    month_references = dict(zip(month_references['Week Start'], month_references['Month']))

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

    def attendance_consolidator(directory):
        return pd.concat([
            attendance_transformer(file) for file in directory.glob('*.csv')
        ])


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

    def segment_df_transform(segment_df):
        segment_df = df.drop(columns = segment_to_drop_columns).copy()
        segment_df = pd.melt(segment_df, id_vars = [
            "Month",
            "Week Start ",
            "Ref",
            "CCP",
            "Manager",
            "Monitored By Name",
            "Employee ID",
            "CCP Name",
            "Team Leader",
            "Tenure",
            "Site",
            "Interaction ID",
            "CHT (sec/s)",
            "Card Type",
            "Monitor Date",
            "Call Reason",
            "Monitoring Date",
            "Monitored By",
            "FIG NEW",
            "Acknowledged"
            ], var_name = 'Call Segment', value_name = 'Value')
        segment_df['Call Segment'] = segment_df['Call Segment'].apply(call_segment_transformation)
        segment_df['Call Segment - Copy'] = segment_df['Call Segment'].apply(call_segment_transformation_sorter)
        return segment_df

    def roster_data_df_merged_transform(attendance_df, roster_data_df):
        roster_data_df_merged = pd.merge(attendance_df, roster_data_df, on = 'Ref', how='left')
        
        roster_data_df_merged = roster_data_df_merged[roster_data_df_merged['Present'] == 1]
        
        roster_data_df_merged['Week Start'] = pd.to_datetime(roster_data_df_merged['Ref'].str.split("-").str[0], format='%m%d%Y')
        roster_data_df_merged['EMPLOYEE'] = roster_data_df_merged['Ref'].str.split("-").str[-1]
        
        roster_data_df_merged = roster_data_df_merged.fillna(0)
        
        roster_data_df_merged['Manager'] = roster_data_df_merged['Manager'].replace({"Wells Jr. David":"Evelyn Wells"})

        roster_data_df_merged['Month'] = roster_data_df_merged['Week Start'].replace(month_references)

        roster_data_df_merged = pd.pivot_table(roster_data_df_merged, index = [
                'CCP Name', 'Site', 'FIG', 'Ref', 'Tenure',
                'Week Start', 'EMPLOYEE',
                'Manager', 'Team Lead', 'Month'
            ], values = [
                'Present',
                'Acknowledged',
                'Monitor Count',
        ], aggfunc = 'sum').reset_index()

        roster_data_df_merged['Present'] = 1

        roster_data_df_merged['Required Monitor'] = np.where(roster_data_df_merged['Site'] == "EL PASO", 1, 2)

        return roster_data_df_merged[[
            'CCP Name',
            'Site',
            'FIG',
            'Present',
            'Required Monitor',
            'Ref',
            'Tenure',
            'Week Start',
            'EMPLOYEE',
            'Acknowledged',
            'Monitor Count',
            'Manager',
            'Team Lead',
            'Month',
        ]]

    result = {}

    def run_raw_data():
        result['df'] = raw_data_consolidator(folder_directory)

    def run_attendance():
        result['attendance_df'] = attendance_consolidator(main_data_dump / 'status')

    t1 = threading.Thread(target=run_raw_data)
    t2 = threading.Thread(target=run_attendance)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    df = result['df']
    attendance_df = result['attendance_df']

    attendance_df['Site'] = attendance_df['Site'].replace({
        "MANILA-TSC":"QUEZON CITY",
        "MANILA-TSCILO":"ILOILO CITY",
        "ELPASO-ELP":"EL PASO",
    })


    aug_df = df[df['Monitoring Date'].dt.month_name() == "August"].copy()
    non_aug_df = df[df['Monitoring Date'].dt.month_name() != 'August'].copy().drop_duplicates(subset = 'Interaction ID')

    df = pd.concat([
        aug_df,
        non_aug_df,
    ])

    df['Monitoring Date'] = pd.to_datetime(df['Monitoring Date'].astype(str).str.split(" ").str[0])

    def get_weight(x):
        if x == 1:
            return 0.17
        return 0

    def new_fig(x):
        if x == "HVCM Hilton Servicing":
            return 'Hilton'
        return x

    categories = [
        "Spoken Words", 
        "Pitch and Tone", 
        "Cross-Talk", 
        "Emotion Detection", 
        "Silence and Hold Time", 
        "Resolution of the Call"
    ]

    questions = {
        "Q1": "OPEN",
        "Q2": "SOLVE",
        "Q3": "DEEPEN RELATIONSHIP",
        "Q4": "CLOSE WITH CONFIDENCE"
    }

    for category in categories:
        for q, stage in questions.items():
            col_name = f"{category} - {stage}"
            source_col = f"{category} - {q}"
            df[col_name] = df[source_col].apply(get_weight)

    columns_map = {
        "Warm Welcome": "Warm Welcome-Opportunities",
        "Human Connection (Listen)": "Human Connection (Listen)-Opportunities",
        "Acknowledge Emotion (Acknowledge)": "Acknowledge Emotion (Acknowledge)-Opportunities",
        "Take Ownership": "Take Ownership-Opportunities",
        "Discover Solutions (Explore)": "Discover Solutions (Explore)-Opportunities",
        "Recognize and Promote Relationship": "Recognize and Promote Relationship-Opportunities",
        "Share a Relevant Offer or Message (Recommend)": "Share a Relevant Offer or Message (Recommend)-Opportunities",
        "Emphasize Resolution/Recap Actions": "Emphasize Resolution/Recap Actions-Opportunities",
        "Show Appreciation": "Show Appreciation-Opportunities"
    }

    for new_col, source_col in columns_map.items():
        df[new_col] = df[source_col].apply(get_value)

    df['Spoken Words - Total - AVERAGE'] = df[[
        'Spoken Words - OPEN', 'Spoken Words - SOLVE', 'Spoken Words - DEEPEN RELATIONSHIP', 'Spoken Words - CLOSE WITH CONFIDENCE'
    ]].mean(axis = 1)

    df['Pitch and Tone - Total - AVERAGE'] = df[[
        'Pitch and Tone - OPEN', 'Pitch and Tone - SOLVE', 'Pitch and Tone - DEEPEN RELATIONSHIP', 'Pitch and Tone - CLOSE WITH CONFIDENCE'
    ]].mean(axis = 1)

    df['Cross-Talk - Total - AVERAGE'] = df[[
        'Cross-Talk - OPEN', 'Cross-Talk - SOLVE', 'Cross-Talk - DEEPEN RELATIONSHIP', 'Cross-Talk - CLOSE WITH CONFIDENCE'
    ]].mean(axis = 1)

    df['Emotion Detection - Total - AVERAGE'] = df[[
        'Emotion Detection - OPEN', 'Emotion Detection - SOLVE', 'Emotion Detection - DEEPEN RELATIONSHIP', 'Emotion Detection - CLOSE WITH CONFIDENCE'
    ]].mean(axis = 1)

    df['Silence and Hold Time - Total - AVERAGE'] = df[[
        'Silence and Hold Time - OPEN', 'Silence and Hold Time - SOLVE', 'Silence and Hold Time - DEEPEN RELATIONSHIP', 'Silence and Hold Time - CLOSE WITH CONFIDENCE'
    ]].mean(axis = 1)

    df['Resolution of the Call - Total - AVERAGE'] = df[[
        'Resolution of the Call - OPEN', 'Resolution of the Call - SOLVE', 'Resolution of the Call - DEEPEN RELATIONSHIP', 'Resolution of the Call - CLOSE WITH CONFIDENCE'
    ]].mean(axis = 1)

    df['Month'] = df['Monitoring Date'].dt.month_name()

    df['Week Start '] = df['Monitoring Date'] - pd.to_timedelta(df['Monitoring Date'].dt.weekday, unit='D')

    df['OPEN - AVERAGE'] = df[[
        'Spoken Words - Q1', 'Pitch and Tone - Q1', 'Cross-Talk - Q1', 'Emotion Detection - Q1', 'Silence and Hold Time - Q1', 'Resolution of the Call - Q1'
    ]].mean(axis = 1)
    df['SOLVE - AVERAGE'] = df[[
        'Spoken Words - Q2', 'Pitch and Tone - Q2', 'Cross-Talk - Q2', 'Emotion Detection - Q2', 'Silence and Hold Time - Q2', 'Resolution of the Call - Q2'
    ]].mean(axis = 1)
    df['DEEPEN RELATIONSHIP - AVERAGE'] = df[[
        'Spoken Words - Q3', 'Pitch and Tone - Q3', 'Cross-Talk - Q3', 'Emotion Detection - Q3', 'Silence and Hold Time - Q3', 'Resolution of the Call - Q3'
    ]].mean(axis = 1)
    df['CLOSE WITH CONFIDENCE - AVERAGE'] = df[[
        'Spoken Words - Q4', 'Pitch and Tone - Q4', 'Cross-Talk - Q4', 'Emotion Detection - Q4', 'Silence and Hold Time - Q4', 'Resolution of the Call - Q4'
    ]].mean(axis = 1)

    df['Q1'] = df[[
        'Spoken Words - Q1', 'Pitch and Tone - Q1', 'Cross-Talk - Q1', 'Emotion Detection - Q1', 'Silence and Hold Time - Q1', 'Resolution of the Call - Q1'
    ]].mean(axis = 1)
    df['Q2'] = df[[
        'Spoken Words - Q2', 'Pitch and Tone - Q2', 'Cross-Talk - Q2', 'Emotion Detection - Q2', 'Silence and Hold Time - Q2', 'Resolution of the Call - Q2'
    ]].mean(axis = 1)
    df['Q3'] = df[[
        'Spoken Words - Q3', 'Pitch and Tone - Q3', 'Cross-Talk - Q3', 'Emotion Detection - Q3', 'Silence and Hold Time - Q3', 'Resolution of the Call - Q3'
    ]].mean(axis = 1)
    df['Q4'] = df[[
        'Spoken Words - Q4', 'Pitch and Tone - Q4', 'Cross-Talk - Q4', 'Emotion Detection - Q4', 'Silence and Hold Time - Q4', 'Resolution of the Call - Q4'
    ]].mean(axis = 1)

    df['Monitor Date'] = df['Monitoring Date']

    df['VG Error - Met'] = df['VG Error Category'].apply(get_value)
    df['Hotline - Met'] = df['Hotline'].apply(get_value)
    df['PP Error - Met'] = df['PP Errors'].apply(get_value)
    df['HOC - Met'] = df['HOC'].apply(get_value)

    df['Error - Met'] = df[[
        'VG Error - Met', 'Hotline - Met', 'HOC - Met', 'PP Error - Met'
    ]].mean(axis = 1)

    df['Listen - Met'] = df['Listen'].apply(get_value)
    df['Acknowledge - Met'] = df['Acknowledge'].apply(get_value)
    df['Explore - Met'] = df['Explore'].apply(get_value)
    df['Recommend - Met'] = df['Recommend'].apply(get_value)

    df['VG LAER - Met'] = df[[
        'Listen - Met', 'Acknowledge - Met', 'Explore - Met', 'Recommend - Met'
    ]].mean(axis = 1)

    df['RTF - Met'] = df['RTF'].apply(get_value)

    df['CFR - Met'] = df['RCR'].apply(get_value)
    df['CHT - Met'] = df['Call Handling Time'].apply(get_value)
    df['Complaints - Met'] = df['Complaints'].apply(get_value)

    df['Employee ID'] = df['Employee ID'].astype(str).str.upper()

    df['Monitored By Name'] = df['Monitored By']

    df['FIG NEW'] = df['FIG'].apply(new_fig)

    df['VIBES Target'] = 0.7

    df['VIBES'] = df[[
        'Q1', 'Q2', 'Q3', 'Q4'
    ]].mean(axis = 1)

    df['Tenure'] = df['Tenure'].replace({"7-10 months":"7-9 months"})

    df['Ref'] = df['Week Start '].dt.strftime('%m%d%Y') + '-' + df['Employee ID']
    df['CCP'] = df['CCP Name']
    df['CCP_1'] = ""
    df['TL'] = ""

    df = df.rename(columns = {
        "Feedback   Is Accepted":"Feedback is Accepted"
    })

    df['Acknowledged'] = np.where(
        ((df['Feedback is Accepted'] == 'Accept By Agent') | (df['Feedback is Accepted'] == 'Accept')) & (~df['Monitored By'].isin(coaches)),
        1,
        0
    )

    def coach_check(r):
        if r in coaches or str(r).strip() in coaches:
            return 0
        return 1

    name_and_card_reference_transformed_data = name_and_card_reference_transformers(main_data_dump = main_data_dump)

    team_leader_reference_df = name_and_card_reference_transformed_data['team_leader_reference_df']
    monitored_by_reference_df = name_and_card_reference_transformed_data['monitored_by_reference_df']
    manager_reference_df = name_and_card_reference_transformed_data['manager_reference_df']
    ccp_name_reference_df = name_and_card_reference_transformed_data['ccp_name_reference_df']
    card_type_refereces_for_vg_toolkit_df = name_and_card_reference_transformed_data['card_type_refereces_for_vg_toolkit_df']

    def get_ccp_name(r):
        return ccp_name_reference_df.get(r)

    df['CCP Name'] = df['Employee ID'].apply(get_ccp_name)
    df = df.replace('=', '', regex=True)

    df['Monitored By'] = df['Monitored By'].str.strip().replace(monitored_by_reference_df)
    df['Team Leader'] = df['Team Leader'].str.strip().replace(team_leader_reference_df)
    df['Manager'] = df['Manager'].str.strip().replace(manager_reference_df)

    df['REF'] = df['Month'] + '-' + df['Monitored By']

    df['TL=Monitored By'] = df['REF'].apply(coach_check)

    df['Monitored By'] = df['Monitored By'].str.replace("Ã±", "n")
    df['Team Leader'] = df['Team Leader'].str.replace("Ã±", "n")
    df['Manager'] = df['Manager'].str.replace("Ã±", "n")

    df = df[~df['REF'].isin(to_delete)]

    df = df.drop(columns = 'REF')

    roster_data_df = pd.pivot_table(df[~df['Monitored By'].isin(coaches)], index = [
        'Ref',
        'Week Start ',
        'Employee ID',
        'Month',
    ], values = ["TL=Monitored By", "Acknowledged"], aggfunc = {"TL=Monitored By":"sum", "Acknowledged":"sum"}).reset_index()

    roster_data_df = roster_data_df.rename(columns = {
        "Employee ID":"EMPLOYEE",
        "Week Start ":"Week Start",
        "TL=Monitored By":"Monitor Count",
        "CCP Name":"Name - BE Roster Management",
        "Team Leader":"L1 Name",
    })

    dump_df_to_save = df.drop(columns = dump_to_drop_columns).copy()

    columns_to_clean = [
        'CHT (sec/s)',
        'OPEN - Call Segment In Seconds',
        'SOLVE - Call Segment In Seconds',
        'DEEPEN RELATIONSHIP - Call Segment In Seconds',
        'CLOSE WITH CONFIDENCE - Call Segment In Seconds'
    ]

    dump_df_to_save[columns_to_clean] = dump_df_to_save[columns_to_clean].applymap(
        lambda x: float(str(x).replace("Secs", "").strip())
    )

    dump_df_to_save = dump_df_to_save[dump_df_to_save['Tenure'] != 'Support']

    df['Month'] = df['Monitoring Date'].dt.strftime('%Y/%m/1')

    attendance_df['Month'] = pd.to_datetime(attendance_df['Month'])

    attendance_df['Ref'] = attendance_df['Week Start'].dt.strftime('%m%d%Y') + '-' + attendance_df['EID']
    attendance_df = attendance_df.drop(columns = ['EID', 'Week Start', 'Month'])

    results = {}

    def run_behavior_transform():
        results['behavior_df'] = behavior_df_transform(df)

    def run_segment_transform():
        results['segment_df'] = segment_df_transform(df)

    def run_roster_transform():
        results['roster_data_df_merged'] = roster_data_df_merged_transform(attendance_df, roster_data_df)

    def run_vg_toolkit_transform():
        results['vg_toolkit_df'] = vg_toolkit_df_transform(df, coaches, card_type_refereces_for_vg_toolkit_df)

    def run_coach_and_qa_transform():
        results['coach_and_qa_df'] = coach_and_qa_transform(df, coach_and_qa_df, month_references)

    threads = [
        threading.Thread(target=run_behavior_transform),
        threading.Thread(target=run_segment_transform),
        threading.Thread(target=run_vg_toolkit_transform),
        threading.Thread(target=run_roster_transform),
        threading.Thread(target=run_coach_and_qa_transform),
    ]

    for t in threads:
        t.start()
        
    for t in threads:
        t.join()

    behavior_df = results['behavior_df']
    segment_df = results['segment_df']
    roster_data_df_merged = results['roster_data_df_merged']
    vg_toolkit_df = results['vg_toolkit_df']
    coach_and_qa_df_transformed = results['coach_and_qa_df']

    tasks = [
        lambda: roster_data_df_merged.to_csv(os.path.join(output_directory, 'roster.csv'), index=False),
        lambda: behavior_df.to_csv(os.path.join(output_directory, "Behavior.csv"), index=False),
        lambda: segment_df.to_csv(os.path.join(output_directory, "Call Segment.csv"), index=False),
        lambda: dump_df_to_save.to_csv(os.path.join(output_directory, "DUMP.csv"), index=False),
        lambda: vg_toolkit_df.to_csv(os.path.join(output_directory, "VG Toolkit.csv"), index=False),
        lambda: coach_and_qa_df_transformed.to_csv(os.path.join(output_directory, "VG and QA Monitoring Adherence.csv"), index=False),
    ]

    threads = [threading.Thread(target=task) for task in tasks]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()

    print(f"Done in {round(end_time - start_time,0)} second(s)")

    pd.DataFrame().to_csv(os.path.join(output_directory, 'trigger', 'refresh.csv'))

    print("Latest Monitoring Data:", max(dump_df_to_save['Monitoring Date']))

    os.startfile(output_directory)
