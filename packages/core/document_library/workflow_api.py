"""Strict v1 workflow adapters. Work is queued; workers are never started here."""
from typing import Literal
from fastapi import Request
from pydantic import BaseModel,ConfigDict,Field
from .catalog_service import CatalogService
from .jobs import JobError
from .auth import AuthError
from .library_api import InputError,_params,_number,_identifier


class Body(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)


class Work(Body):
    title_display:str=Field(min_length=1,max_length=500)
    author:str|None=Field(default=None,min_length=1,max_length=200)
    item_type:Literal['novel','webtoon','comic','document','unknown']='unknown'
    publication_state:Literal['completed','ongoing','unknown']='unknown'


class Edition(Body):
    edition_key:str=Field(min_length=1,max_length=128)
    edition_label:str=Field(min_length=1,max_length=200)
    representation:Literal['text','imagepack','document','single_image']
    source_form:Literal['born_digital','scan','unknown']='unknown'
    language:str|None=Field(default=None,min_length=1,max_length=64)
    source_series:str|None=Field(default=None,min_length=1,max_length=200)


class Classification(Body):
    item_type:Literal['novel','webtoon','comic','document','unknown']
    publication_state:Literal['completed','ongoing','unknown']
    expected_item_type:Literal['novel','webtoon','comic','document','unknown']
    expected_publication_state:Literal['completed','ongoing','unknown']
    evidence:str=Field(min_length=1,max_length=500)


class Binding(Body):
    edition_id:str=Field(min_length=1,max_length=200)
    expected_revision_id:str|None=Field(default=None,min_length=1,max_length=200)


class Rights(Body):
    rights_status:Literal['allowed','review','blocked']
    rights_receipt:str=Field(min_length=1,max_length=200)


def install_workflow_routes(app, ingestion, credentials, supervisor):
    catalog=CatalogService(ingestion)

    def params(request, *, search=False):
        values=_params(request,scalar=('cursor','limit','q') if search else ('cursor','limit'))
        if 'limit' in values:values['limit']=_number(values['limit'],low=1,high=100)
        supervisor.admit()
        return values

    def mutate(request, callback):
        _params(request);supervisor.admit()
        try:
            return callback(*credentials(request))
        except JobError:
            raise AuthError('ASSET_UNAVAILABLE',409) from None
        except ValueError:
            raise InputError() from None

    @app.get('/api/v1/upload-batches')
    def batches(request:Request):
        return catalog.batches(credentials(request)[0],**params(request))

    @app.get('/api/v1/admin/uploads/review-queue')
    def review_queue(request:Request):
        return catalog.review_queue(credentials(request)[0],**params(request))

    @app.get('/api/v1/admin/catalog/works')
    def works(request:Request):
        return catalog.works(credentials(request)[0],**params(request,search=True))

    @app.get('/api/v1/admin/catalog/works/{work_id}/editions')
    def editions(work_id:str,request:Request):
        return catalog.editions(credentials(request)[0],work_id,**params(request))

    @app.put('/api/v1/admin/catalog/works/{work_id}')
    def work(work_id:str,body:Work,request:Request):
        return mutate(request,lambda token,csrf:catalog.put_work(token,csrf,work_id,**body.model_dump()))

    @app.put('/api/v1/admin/catalog/works/{work_id}/editions/{edition_id}')
    def edition(work_id:str,edition_id:str,body:Edition,request:Request):
        return mutate(request,lambda token,csrf:catalog.put_edition(token,csrf,work_id,edition_id,**body.model_dump()))

    @app.put('/api/v1/admin/catalog/works/{work_id}/classification/{request_id}')
    def classify(work_id:str,request_id:str,body:Classification,request:Request):
        return mutate(request,lambda token,csrf:catalog.classify(token,csrf,work_id,request_id,**body.model_dump()))

    def bind(request,upload_id,body,*,admin):
        _identifier(body.edition_id)
        if body.expected_revision_id is not None:_identifier(body.expected_revision_id)
        def run(token,csrf):
            callback=ingestion.bind_upload if admin else ingestion.enqueue_upload
            callback(token,csrf,upload_id=upload_id,**body.model_dump())
            return catalog.upload_status(token,upload_id,admin=admin)
        return mutate(request,run)

    @app.post('/api/v1/uploads/{upload_id}/enqueue')
    def enqueue(upload_id:str,body:Binding,request:Request):
        return bind(request,upload_id,body,admin=False)

    @app.post('/api/v1/admin/uploads/{upload_id}/bind')
    def admin_bind(upload_id:str,body:Binding,request:Request):
        return bind(request,upload_id,body,admin=True)

    @app.post('/api/v1/admin/uploads/{upload_id}/rights-review')
    def rights(upload_id:str,body:Rights,request:Request):
        def run(token,csrf):
            ingestion.review_upload(token,csrf,upload_id,**body.model_dump())
            return catalog.upload_status(token,upload_id,admin=True)
        return mutate(request,run)

    app.state.catalog=catalog
