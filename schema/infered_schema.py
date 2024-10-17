import pandera as pa
from pandera.typing import Series


class Report(pa.DataFrameModel):
    PERIODO = Series[str]
    ESTABELECIMENTO: Series[str]
    CNPJ: Series[int]
    # Nome_Fantasia: Series[str] = pa.Field(alias="Nome Fantasia", nullable=True, required=False)
    # Tipo_de_Estabelecimento: Series[str] = pa.Field(alias="Tipo de Estabelecimento")
    # Natureza_Jurídica: Series[str] = pa.Field(alias="Natureza Jurídica", required=False)
    # Porte: Series[str] = pa.Field(alias="Porte")
    # Endereco_Completo_Receita_Federal: Series[str] = pa.Field(alias="Endereço Completo Receita Federal", required=False)
    UF: Series[str]
    MUNICIPIO: Series[str]
    ESPECIALIDADE: Series[str]

    class Config:
        coerce = True
