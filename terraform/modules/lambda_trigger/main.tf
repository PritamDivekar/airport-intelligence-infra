resource "aws_lambda_function" "s3_raw_trigger" {
  function_name = "airport-intelligence-raw-trigger"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.10"
  handler       = "lambda_function.lambda_handler"

  filename         = "${path.module}/lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda.zip")

  environment {
    variables = {
      STEP_FUNCTION_ARN = var.step_function_arn
    }
  }
}
