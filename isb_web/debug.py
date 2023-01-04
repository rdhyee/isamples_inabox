import logging
from typing import Any

import fastapi
from fastapi import FastAPI, HTTPException
from isamples_metadata.SESARTransformer import SESARTransformer

from isamples_metadata.OpenContextTransformer import OpenContextTransformer

from isamples_metadata.GEOMETransformer import GEOMETransformer

from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer
from pydantic import BaseModel

from isamples_metadata.metadata_exceptions import MetadataException
from isb_web.isb_enums import ISBAuthority, ISBReturnField

debug_api = FastAPI()
# dao: Optional[SQLModelDAO] = None
DEBUG_PREFIX = "/debug"
logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("manage")


class DebugTransformParams(BaseModel):
    input_record: dict
    authority: ISBAuthority
    return_field: ISBReturnField


@debug_api.get("/", include_in_schema=False)
async def root(request: fastapi.Request):
    return fastapi.responses.RedirectResponse(url=f"{request.scope.get('root_path')}/docs")


@debug_api.post("/debug_transform")
def debug_transform(params: DebugTransformParams) -> Any:
    """Runs the transform for the specified input document
    Args:
        params: Class that contains the credentials and the data to post to datacite
    Return: The result of running the transformer
    """
    try:
        if params.authority == ISBAuthority.GEOME:
            transformed = GEOMETransformer(params.input_record).transform()
        elif params.authority == ISBAuthority.OPENCONTEXT:
            transformed = OpenContextTransformer(params.input_record).transform()
        elif params.authority == ISBAuthority.SESAR:
            transformed = SESARTransformer(params.input_record).transform()
        elif params.authority == ISBAuthority.SMITHSONIAN:
            transformed = SmithsonianTransformer(params.input_record).transform()
        else:
            raise HTTPException(500, "Specified authority is not an input we can transform")
    except MetadataException as e:
        raise HTTPException(415, str(e))

    if params.return_field != ISBReturnField.ALL:
        key = params.return_field.dictionary_key()
        confidence_key = f"{key}Confidence"
        return {
            key: transformed.get(key),
            confidence_key: transformed.get(confidence_key)
        }
    else:
        return transformed
