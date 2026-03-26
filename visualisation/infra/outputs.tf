output "function_url" {
  description = "Public URL for the tile server"
  value       = aws_lambda_function_url.app.function_url
}

output "ecr_repository_url" {
  description = "ECR repository URL for pushing images"
  value       = aws_ecr_repository.app.repository_url
}
