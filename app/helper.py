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
