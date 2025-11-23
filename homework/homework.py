"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    import pandas as pd
    import zipfile
    import os
    from glob import glob

    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    input_path = "files/input/"
    output_path = "files/output/"
    
    os.makedirs(output_path, exist_ok=True)
    
    zip_files = glob(os.path.join(input_path, "*.zip"))
    
    dfs = []
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
            for csv_file in csv_files:
                with z.open(csv_file) as f:
                    df = pd.read_csv(f)
                    dfs.append(df)
    
    data = pd.concat(dfs, ignore_index=True)
    
    data['client_id'] = range(1, len(data) + 1)
    
    client = pd.DataFrame()
    client['client_id'] = data['client_id']
    client['age'] = data['age']
    client['job'] = data['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    client['marital'] = data['marital']
    client['education'] = data['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA)
    client['credit_default'] = (data['credit_default'] == 'yes').astype(int)
    client['mortgage'] = (data['mortgage'] == 'yes').astype(int)
    
    campaign = pd.DataFrame()
    campaign['client_id'] = data['client_id']
    campaign['number_contacts'] = data['number_contacts']
    campaign['contact_duration'] = data['contact_duration']
    campaign['previous_campaign_contacts'] = data['previous_campaign_contacts']
    campaign['previous_outcome'] = (data['previous_outcome'] == 'success').astype(int)
    campaign['campaign_outcome'] = (data['campaign_outcome'] == 'yes').astype(int)
    
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    campaign['last_contact_date'] = data.apply(
        lambda row: f"2022-{month_map[row['month']]:02d}-{int(row['day']):02d}",
        axis=1
    )
    
    economics = pd.DataFrame()
    economics['client_id'] = data['client_id']
    economics['cons_price_idx'] = data['cons_price_idx']
    economics['euribor_three_months'] = data['euribor_three_months']
    
    client.to_csv(os.path.join(output_path, 'client.csv'), index=False)
    campaign.to_csv(os.path.join(output_path, 'campaign.csv'), index=False)
    economics.to_csv(os.path.join(output_path, 'economics.csv'), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
