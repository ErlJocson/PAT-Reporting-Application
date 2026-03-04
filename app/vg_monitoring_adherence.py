import numpy as np
import pandas as pd

def coach_and_qa_transform(df, reference_df, month_references):
    df['new_ref'] = df['Monitoring Date'].dt.month_name() + df['Monitoring Date'].dt.year.astype(str) + df['Monitored By']

    df = df[df['Month'] >= "2025-12-01"] 

    merged_df = pd.merge(df, reference_df, left_on = 'new_ref', right_on = 'ref', how = 'left')

    merged_df = merged_df[merged_df['Position'].notna()]

    merged_df['Feedback is Accepted'] = np.where(
        ((merged_df['Feedback is Accepted'] == 'Accept By Agent') | (merged_df['Feedback is Accepted'] == 'Accept')),
        1,
        0
    )

    pivot_df = pd.pivot_table(merged_df, index = ['Week Start ', 'Monitored By', 'new_ref', 'Position'], values = ['Interaction ID', 'Feedback is Accepted'], aggfunc = {
        "Interaction ID":"count",
        "Feedback is Accepted":"sum",
    }).reset_index()

    reference_df = reference_df[['ref', 'Monitor Target']]

    pivot_df_with_target = pd.merge(pivot_df, reference_df, left_on = 'new_ref', right_on = 'ref')

    pivot_df_with_target = pivot_df_with_target.drop(columns = ['new_ref', 'ref'])

    pivot_df_with_target['Month'] = pivot_df_with_target['Week Start '].replace(month_references)

    pivot_df_with_target = pivot_df_with_target.rename({
        "Interaction ID":"Monitor Count",
    })

    return pivot_df_with_target
