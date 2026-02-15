# Deployment Pipeline

Detail the process of setting up a deployment pipeline for the code-script application with the following environments:

1. **Development (Dev):**
   - Deploy the application with development configuration.
   - Allow for rapid iteration and testing of new features.
   - Use separate development resources (e.g., databases, storage).

2. **Staging:**
   - Mirror the production environment as closely as possible.
   - Serve as the primary environment for QA after internal Dev testing.
   - Use independent staging resources.

3. **User Acceptance Testing (UAT):**
   - Deploy the code for final user/client validation before going live.
   - Use UAT-specific settings and resources.
   - Validate workflows with production-like data, but ensure UAT data is not mixed with live/production data.

4. **Production (Prod):**
   - Deploy only validated and approved code.
   - Use production resources, hosting on the OAIT Solutions domain.
   - Strictly separate production data from data in Dev, Staging, and UAT.
   - Enforce environment variable/configuration practices to ensure no data leakage.

**Requirements:**
- Each environment must have its own deployment configuration and resources.
- Database, storage, and environment variables for production must remain isolated from lower environments.
- Document the deployment workflow, e.g., using CI/CD (GitHub Actions, Azure Pipelines, etc.).
- Review security and backup strategies for each environment.
- Ensure DNS and SSL configurations are set for OAIT Solutions domain in production.