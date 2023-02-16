import pandas as pd

def load_label_data(as_of_date, as_of_end_date, con):
    """
    Query Utilization table to create label data for er visits
    Where clause @ line 27 ensures patient is alive at the as_of_date
    Args:
        as_of_date: datetime.date object designating point in history from where the model will be run
        as_of_end_date: datetime.date object designating end of timeframe we are evaluating the model
        con: sqlite3 connection object
    Returns: 
        pd.read_sql_query(sql, con): pandas.DataFrame containing label data
    """

    sql = f"""
    SELECT patient_id,
           CASE WHEN er_visits = 0 THEN 0 ELSE 1 END AS er_visit
    FROM
    
    (SELECT `Patient Id` AS patient_id, 
            SUM(ER_Visits) AS er_visits
    FROM Utilization
    WHERE Date >= '{str(as_of_date)}' AND
          Date < '{str(as_of_end_date)}'
    GROUP BY `Patient Id`) A
    
    WHERE patient_id IN
    
    (SELECT `Patient Id` AS patient_id
    FROM Demographics
    WHERE `Deceased Date` IS NULL OR
          `Deceased Date` < '{str(as_of_date)}')
    """

    return pd.read_sql_query(sql, con)

def load_procedure_features(feature_start_date, feature_end_date, con):
    """
    Query procedure table to create procedure features
    Args:
        feature_start_date: datetime.date object designating start of feature time range
        feature_end_date: datetime.date object designating end of feature time range
        con: sqlite3 connection object
    Returns: 
        pd.read_sql_query(sql, con): pandas.DataFrame containing procedure feature data
    """

    sql = f"""
    SELECT `Patient Id` AS patient_id,
           SUM(`Operations on the endocrine system`) AS endocrine_ops,
           SUM(`Miscellaneous diagnostic and therapeutic procedures`) AS misc_proc,
           SUM(`Operations on the ear`) AS ear_proc,
           SUM(`Operations on the hemic and lymphatic system`) AS hemic_lymphatic_proc,
           SUM(`Operations on the respiratory system`) AS respiratory_proc,
           SUM(`Operations on the cardiovascular system`) AS cardiovascular_proc,
           SUM(`Operations on the eye`) AS eye_proc,
           SUM(`Operations on the integumentary system`) AS integumentary_proc,
           SUM(`Operations on the urinary system`) AS urinary_proc,
           SUM(`Operations on the nervous system`) AS nervous_proc,
           SUM(`Operations on the male genital organs`) AS male_proc,
           SUM(`Operations on the musculoskeletal system`) AS musculoskeletal_proc,
           SUM(`Operations on the digestive system`) AS digestive_proc,
           SUM(`Obstetrical procedures`) AS obstetrical_proc,
           SUM(`Operations on the female genital organs`) AS female_proc,
           SUM(`Operations on the nose_ mouth_ and pharynx`) AS nose_mouth_pharynx_proc
    FROM Procedure
    WHERE Date > '{str(feature_start_date)}' AND
          Date <= '{str(feature_end_date)}'
    GROUP BY `Patient Id`
    """

    return pd.read_sql_query(sql, con)

def load_diagnosis_features(feature_start_date, feature_end_date, con):
    """
    Query Diagnosis table to create Diagnosis features
    Args:
        feature_start_date: datetime.date object designating start of feature time range
        feature_end_date: datetime.date object designating end of feature time range
        con: sqlite3 connection object
    Returns: 
        pd.read_sql_query(sql, con): pandas.DataFrame containing Diagnosis feature data
    """
    
    sql = f"""
    SELECT `Patient Id` AS patient_id,
           SUM(`Diseases of the digestive system`) AS digestive_dia,
           SUM(`Diseases of the nervous system and sense organs`) AS nervous_sense_dia,
           SUM(`Diseases of the musculoskeletal system and connective tissue`) AS musculoskeletal_connective_dia,
           SUM(`Endocrine_ nutritional_ and metabolic diseases and immunity disorders`) AS endocrine_metabolic_immuno_dia,
           SUM(`Residual codes_ unclassified_ all E codes`) AS other_icd_dia,
           SUM(`Certain conditions originating in the perinatal period`) AS perinatal_dia,
           SUM(`Congenital anomalies`) AS congenital_dia,
           SUM(`Neoplasms`) AS neoplasms_dia,
           SUM(`Infectious and parasitic diseases`) AS infect_parisitic_dia,
           SUM(`Diseases of the skin and subcutaneous tissue`) AS skin_dia,
           SUM(`Diseases of the blood and blood-forming organs`) AS blood_dia,
           SUM(`Mental illness`) AS mental_dia,
           SUM(`Injury and poisoning`) AS inj_poison_dia,
           SUM(`Complications of pregnancy_ childbirth_ and the puerperium`) AS pregnancy_puerperium_dia,
           SUM(`Symptoms_ signs_ and ill-defined conditions and factors influencing health status`) AS ill_defined_dia,
           SUM(`Diseases of the respiratory system`) AS respiratory_dia,
           SUM(`Diseases of the circulatory system`) AS circulatory_dia,
           SUM(`Diseases of the genitourinary system`) AS genitourinary_dia
    FROM Diagnosis
    WHERE Date > '{str(feature_start_date)}' AND
          Date <= '{str(feature_end_date)}'
    GROUP BY `Patient Id`
    """

    return pd.read_sql_query(sql, con)

def load_demographics_features(as_of_date, con):
    """
    Query demographics table to create demographics features
    Args:
        as_of_date_str: string object designating point in history from where the model will be run
        con: sqlite3 connection object
    Returns: 
        pd.read_sql_query(sql, con): pandas.DataFrame containing Demographics feature data
    """
    
    sql = f"""
    SELECT `Patient Id` AS patient_id,
           CASE WHEN Gender = 'female' THEN 1 ELSE 0 END AS female_bool,
           `Birth Year` AS birth_year
    FROM Demographics
    WHERE `Deceased Date` IS NULL OR
          `Deceased Date` < '{str(as_of_date)}'
    """

    return pd.read_sql_query(sql, con)

def load_utilization_features(feature_start_date, feature_end_date, con):
    """
    Query Utilization table to create Utilization features
    Args:
        feature_start_date: datetime.date object designating start of feature time range
        feature_end_date: datetime.date object designating end of feature time range
        con: sqlite3 connection object
    Returns: 
        pd.read_sql_query(sql, con): pandas.DataFrame containing Utilization feature data
    """
    
    sql = f"""
    SELECT `Patient Id` AS patient_id,
           SUM(Office_Visits) AS off_visits_util,
           SUM(ER_Visits) AS er_visits_util,
           SUM(Admits) AS admits_util
    FROM Utilization
    WHERE Date > '{str(feature_start_date)}' AND
          Date <= '{str(feature_end_date)}'
    GROUP BY `Patient Id`
    """

    return pd.read_sql_query(sql, con)

def rename_feature_dfs(feature_df, month_val):
    """
    Rename time range calculated features to indicate the time range
    For example: digestive_dia, the count of digestive diagnoses in the 
    diagnosis feature query would be renamed to 3_month_digestive_dia 
    for the table where the difference between the feature start date and
    feature end date is 3 months
    Args:
        feature_df: pandas.DataFrame containing feature data
        month_val: int specifying the how many months back the feature calculates
        con: sqlite3 connection object
    Returns: 
        feature_df: pandas.DataFrame containing feature data with renamed columns
    """
    
    for col_name in feature_df.columns:
        if col_name != 'patient_id':
            feature_df.rename(columns={col_name:str(month_val) + '_month_' + col_name}, inplace=True)
            
    return feature_df