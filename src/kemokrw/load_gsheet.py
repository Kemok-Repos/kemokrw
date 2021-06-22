from kemokrw.load import Load


class LoadGSheet(Load):
    """"Clase LoadGSheet implementación de la clase Load.

    Cumple la función de cargar información a una hoja de Google Sheets.

    Atributos
    ---------
    spreadsheet_id : str
    sheet : str
    model : dict
    metadata : dict

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla en Google Sheets.
    save_data():
        Almacena la data de un pandas.DataFrame Object una tabla en Google Sheets.
    """
    pass