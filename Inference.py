from inference_sdk import InferenceHTTPClient

CLIENT = InferenceHTTPClient(
    api_url="https://serverless.roboflow.com",
    api_key="API_KEY"
)

result = CLIENT.infer(your_image.jpg, model_id="plantdoc_yolo/1")