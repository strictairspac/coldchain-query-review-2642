from fastapi import APIRouter,Depends
from ..config.database import get_db
from ..schemas.correction import ImportInput
from ..services.auth import current_user,writer
from ..services.correction import list_imports,get_import,import_corrections
router=APIRouter(prefix='/imports')
@router.get('')
def listing(db=Depends(get_db),user=Depends(current_user)):return list_imports(db)
@router.get('/{import_id}')
def detail(import_id:int,db=Depends(get_db),user=Depends(current_user)):return get_import(db,import_id)
@router.post('',status_code=201)
def create(data:ImportInput,db=Depends(get_db),user=Depends(writer)):return import_corrections(db,data,actor=user.email)
