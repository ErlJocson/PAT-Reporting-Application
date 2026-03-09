import pandas as pd

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