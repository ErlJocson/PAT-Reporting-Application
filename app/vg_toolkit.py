from app.helper import (
    vg_main_components_transform,
    vg_main_components_sorter,
    get_value,
    vg_sub_components_sorter,
    accept_rate,
    decline_rate,
    not_applicable_rate,
    vg_coaches_transform,
    tenure_sorter_transform
)
import pandas as pd

def vg_toolkit_df_transform(df, coaches, card_type_refereces_for_vg_toolkit_df):
    df = df[df['Monitoring Date'] > '2025-10-31']

    df = df[[
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
        "CCP_1",
        "Acknowledged",
        "Explore CVP and Identify Clues",
        "Listen and Discover",
        "Connect a Relevant Offer",
        "Transition with a Relevant Statement",
        "Communicate Features of the Offer",
        "Personalize Value and Benefit",
        "Process the Offer Compliantly",
        "Offer Status",
        "Offer Type", 
    ]]

    melted_df = pd.melt(
        df,
        var_name='Sub VG Components',
        value_name='Sub VG Components Value',
        id_vars=[
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
            "CCP_1",
            "Acknowledged",
            "Offer Type",
            'Offer Status'
        ]
    )

    melted_df['VG Component'] = melted_df['Sub VG Components'].apply(vg_main_components_transform)
    melted_df['VG Component - Sorter'] = melted_df['Sub VG Components'].apply(vg_main_components_sorter)
    melted_df['Sub VG Components - Sorter'] = melted_df['Sub VG Components'].apply(vg_sub_components_sorter)

    melted_df['Accept Rate'] = melted_df['Offer Status'].apply(accept_rate)
    melted_df['Decline Rate'] = melted_df['Offer Status'].apply(decline_rate)
    melted_df['Not Applicable Rate'] = melted_df['Offer Status'].apply(not_applicable_rate)

    melted_df['Is VG Coach'] = (melted_df['Monitoring Date'].dt.month_name() + "-" + melted_df['Monitored By']).apply(lambda r: vg_coaches_transform(r, coaches))

    melted_df['Sub VG Components Value %'] = melted_df['Sub VG Components Value'].apply(get_value)
    melted_df['Tenure Sorter'] = melted_df['Tenure'].apply(tenure_sorter_transform)
    melted_df['Card Type L1'] = melted_df['Card Type'].apply(lambda r: card_type_refereces_for_vg_toolkit_df.get(r))

    return melted_df
