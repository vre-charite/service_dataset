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

import uuid

# admin will have all the permission to access 
# all the name folder under the bucket
def create_dataset_policy_template(dataset_code, content=None):
    if not content:
        content = '''
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["s3:ListBucket", "s3:GetObject","s3:PutObject","s3:GetBucketLocation", "s3:DeleteObject"],
                    "Effect": "Allow",
                    "Resource": ["arn:aws:s3:::%s"]
                }
            ]
        }
        '''%(dataset_code)

    # now create the template file since we need to use the file
    # with minio admin client to create policy
    # since here we will write to disk. to avoid collision use the uuid4
    template_name = str(uuid.uuid4())+".json"
    policy_file = open(template_name, "w")
    policy_file.write(content)
    policy_file.close()

    return template_name