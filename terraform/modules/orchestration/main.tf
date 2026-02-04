resource "aws_sfn_state_machine" "airport_pipeline" {
  name     = "airport-intelligence-etl-pipeline"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Airport Intelligence Glue ETL Pipeline with Retry + Concurrency Handling"

    StartAt = "ETL_Customers"

    States = {

      ETL_Customers = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Parameters = {
          JobName = "ETL_Customers_v2"
        }

        Retry = [
          {
            ErrorEquals = [
              "Glue.ConcurrentRunsExceededException",
              "Glue.ThrottlingException"
            ]
            IntervalSeconds = 60
            MaxAttempts     = 10
            BackoffRate     = 2.0
          }
        ]

        Next = "ETL_Operational_Health"
      }

      ETL_Operational_Health = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Parameters = {
          JobName = "Kpi_2_v2"
        }

        Retry = [
          {
            ErrorEquals = [
              "Glue.ConcurrentRunsExceededException",
              "Glue.ThrottlingException"
            ]
            IntervalSeconds = 60
            MaxAttempts     = 10
            BackoffRate     = 2.0
          }
        ]

        Next = "Run_Crawlers"
      }

      Run_Crawlers = {
        Type = "Parallel"

        Branches = [

          {
            StartAt = "Customers_Crawler"
            States = {
              Customers_Crawler = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"

                Parameters = {
                  Name = "Customers_table_crawler_v2"
                }

                End = true
              }
            }
          },

          {
            StartAt = "Kpi_Crawler"
            States = {
              Kpi_Crawler = {
                Type     = "Task"
                Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"

                Parameters = {
                  Name = "Kpi_2_Crawler_v2"
                }

                End = true
              }
            }
          }

        ]

        End = true
      }
    }
  })
}
