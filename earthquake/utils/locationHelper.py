import cpca
def extract_pos(address,property):
    # cpca.transform returns a Pandas DataFrame
    df = cpca.transform([address])
    # Extract the province from the first row
    province = df.iloc[0][property]  
    return province if not province is None else address
