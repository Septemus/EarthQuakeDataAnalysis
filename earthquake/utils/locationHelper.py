import cpca
def extract_pos(address,property):
    if(address == "渤海" or address == "黄海" or address=="南海"):
        address+="海域"
    # cpca.transform returns a Pandas DataFrame
    df = cpca.transform([address])
    # Extract the province from the first row
    province = df.iloc[0][property]  
    return province if not province is None else address
