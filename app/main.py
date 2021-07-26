from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import ConfigClass
from .api_registry import api_registry
from .consumer.consumers import dataset_consumer


def create_app():
    '''
    create app function
    '''
    app = FastAPI(
        title="Service Dataset",
        description="Service Dataset",
        docs_url="/v1/api-doc",
        version=ConfigClass.version
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins="*",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API registry
    # v1
    api_registry(app)
    dataset_consumer()

    return app
