import cpca
def extract_province(address):
    # cpca.transform returns a Pandas DataFrame
    df = cpca.transform([address])
    # Extract the province from the first row
    return df.iloc[0]['省']  # '省' is the column name for province
