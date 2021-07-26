class DatasetValidator:
    
    @staticmethod
    def get(key):
        return {
            "code": DatasetValidator.code,
            "title": DatasetValidator.title,
            "authors": DatasetValidator.authors,
            "type": DatasetValidator.type,
            "modality": DatasetValidator.modality,
            "collection_method": DatasetValidator.collection_methods,
            "license": DatasetValidator.license,
            "tags": DatasetValidator.tags,
            "description": DatasetValidator.desc
        }.get(key, lambda v: v is not None)

    @staticmethod
    def code(value: str):
        validated = True
        if any(ele.isupper() for ele in value):
            validated = False
        if not value.isalnum():
            validated = False
        if " " in value:
            validated = False
        if len(value) > 32:
            validated = False
        return validated

    @staticmethod
    def title(value):
        validated = True
        if len(value) > 100:
            validated = False
        return validated

    @staticmethod
    def authors(value):
        validated = True
        if not isinstance(value, list):
            print(value)
            validated = False
        if len(set(value)) != len(value):
            validated = False
        if len(value) > 10:
            validated = False
        return validated

    @staticmethod
    def desc(value):
        validated = True
        if len(value) > 5000:
            validated = False
        return validated

    @staticmethod
    def modality(value: list):
        allowed = [
            "anatomical approach",
            "neuroimaging",
            "microscopy",
            "histological approach",
            "neural connectivity",
            "molecular expression characterization",
            "multimodal approach", 
            "electrophysiology",
            "behavioral approach",
            "molecular expression approach",
            "cell population imaging",
            "physiological approach", 
            "morphological approach", 
            "cell morphology", 
            "cell counting",
            "cell population characterization",
            "computational modeling"]
        validated = True
        for e in value:
            if e not in allowed:
                validated = False
        return validated

    @staticmethod
    def type(value):
        allowed = ["GENERAL", "BIDS"]
        validated = True
        if value not in allowed:
            validated = False
        return validated

    @staticmethod
    def tags(value):
        validated = True
        if not isinstance(value, list):
            validated = False
        if len(set(value)) != len(value):
            validated = False
        for val in value:
            if " " in val:
                validated = False
        if len(value) > 10:
            validated = False
        return validated

    @staticmethod
    def collection_methods(value):
        validated = True
        if not isinstance(value, list):
            validated = False
        if len(set(value)) != len(value):
            validated = False
        for val in value:
            if " " in val:
                validated = False
        if len(value) > 10:
            validated = False
        return validated

    @staticmethod
    def license(value):
        validated = True
        if len(value) > 20:
            validated = False
        return validated