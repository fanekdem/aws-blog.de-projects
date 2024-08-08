#!/usr/bin/env python3
import os

import aws_cdk as cdk

from stacks.main_stack import RAGStack


app = cdk.App()
RAGStack(
    app,
    "LowCostServerlessRAGStack",
)

app.synth()
