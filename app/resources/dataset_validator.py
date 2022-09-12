# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

import re

validator_messages = {
    "QUICK_VALIDATION_FAILED": "Quick validation failed - the general folder structure does not resemble a BIDS dataset. \
        Have you chosen the right folder (with 'sub-*/' subfolders)? Check for structural/naming issues and presence of at least one subject.",
    "NOT_INCLUDED": "Files with such naming scheme are not part of BIDS specification. \
        This error is most commonly caused by typos in file names that make them not BIDS compatible. \
            Please consult the specification and make sure your files are named correctly. \
                If this is not a file naming issue (for example when including files not yet covered by the BIDS specification) \
                    you should include a '/.bidsignore' file in your dataset (see https://github.com/bids-standard/bids-validator#bidsignore for details). \
                        Please note that derived (processed) data should be placed in /derivatives folder and source data (such as DICOMS or behavioural logs in proprietary formats) should be placed in the /sourcedata folder.",
    "SUBJECT_FOLDERS": "There are no subject folders (labeled 'sub-*') in the root of this dataset.",
    "DATASET_DESCRIPTION_JSON_MISSING": "The compulsory file /dataset_description.json is missing.",
    "FILE_READ": "We were unable to read this file. Make sure it contains data (file size > 0 kB) and is not corrupted, incorrectly named, or incorrectly symlinked.",
    "EMPTY_FILE": "Empty files not allowed.",
    "DATASET_DESCRIPTION_JSON_MISSING": "The compulsory file /dataset_description.json is missing.",
    "README_FILE_MISSING": "The recommended file /README is missing."
}
