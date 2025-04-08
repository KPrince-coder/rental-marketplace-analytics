# Redshift Setup Troubleshooting Guide

This guide provides solutions for common issues encountered when setting up the Redshift cluster and schema for the Rental Marketplace Analytics Pipeline.

## Common Issues and Solutions

### 1. Redshift Cluster Creation Fails

#### Issue: Parameter validation errors

```
Parameter validation failed:
Invalid type for parameter NumberOfNodes, value: None, type: <class 'NoneType'>, valid types: <class 'int'>
```

**Solution**: This issue has been fixed in the latest version of the code. If you encounter it, make sure you're using the latest version of `create_redshift_cluster.py`.

#### Issue: Insufficient permissions

```
An error occurred (AccessDenied) when calling the CreateCluster operation: User: arn:aws:iam::123456789012:user/username is not authorized to perform: redshift:CreateCluster
```

**Solution**: Ensure your AWS user has the necessary permissions to create Redshift clusters. Add the following policies to your IAM user:
- `AmazonRedshiftFullAccess`
- `AmazonS3ReadOnlyAccess`
- `IAMFullAccess` (or more restricted permissions to create/modify roles)

#### Issue: Service limit exceeded

```
An error occurred (ClusterQuotaExceeded) when calling the CreateCluster operation: Cluster quota exceeded.
```

**Solution**: You've reached the limit of Redshift clusters for your AWS account. Either:
1. Delete unused Redshift clusters
2. Request a quota increase from AWS
3. Use an existing cluster by modifying the cluster identifier in the script

### 2. Security Group Issues

#### Issue: Cannot connect to Redshift

If you can't connect to Redshift from your local machine or other AWS services, it might be a security group issue.

**Solution**:
1. Run the script with the `--setup-redshift` option which automatically configures the security group:
   ```bash
   python main.py --setup-redshift
   ```

2. Or manually update the security group:
   ```bash
   python scripts/create_redshift_cluster.py
   ```
   This script includes a function to update the security group.

3. Check the security group in the AWS Console:
   - Go to EC2 > Security Groups
   - Find the security group associated with your Redshift cluster
   - Ensure it has an inbound rule allowing TCP traffic on port 5439

### 3. IAM Role Issues

#### Issue: Redshift cannot access S3

```
An error occurred when calling the CreateCluster operation: User: arn:aws:iam::123456789012:user/username is not authorized to perform: iam:CreateRole
```

**Solution**:
1. Ensure your AWS user has permissions to create IAM roles
2. If you can't get these permissions, create the role manually:
   - Create a role with the trust policy allowing Redshift to assume it
   - Attach the `AmazonS3ReadOnlyAccess` policy to the role
   - Update the script to use the ARN of this role

### 4. Schema Creation Issues

#### Issue: Cannot connect to Redshift for schema creation

```
Failed to connect to Redshift: could not connect to server: Connection timed out
```

**Solution**:
1. Ensure the Redshift cluster is fully available (status: "available")
2. Check that the security group allows connections from your IP address
3. Verify the endpoint and port in your `.env` file match the actual Redshift cluster

#### Issue: SQL errors during schema creation

```
Failed to execute SQL file: syntax error at or near "CREATE"
```

**Solution**:
1. Check the SQL files in the `sql/redshift/` directory for syntax errors
2. Ensure the SQL is compatible with Redshift (some MySQL syntax may not work)
3. Try executing the SQL files manually using a tool like pgAdmin or psql

### 5. Waiting for Cluster Availability

#### Issue: Script times out waiting for cluster to be available

The script might appear to hang while waiting for the Redshift cluster to become available.

**Solution**:
1. Be patient - creating a Redshift cluster can take 5-10 minutes
2. Check the AWS Console to monitor the cluster status
3. If the script fails, you can run it again - it will detect the existing cluster

## Advanced Troubleshooting

### Checking Cluster Status

To check the status of your Redshift cluster:

```bash
aws redshift describe-clusters --cluster-identifier rental-marketplace-redshift
```

### Manually Creating a Cluster

If the script consistently fails, you can create the cluster manually:

1. Go to the AWS Console > Redshift
2. Click "Create cluster"
3. Use the following settings:
   - Cluster identifier: rental-marketplace-redshift
   - Node type: dc2.large
   - Number of nodes: 1
   - Database name: rental_marketplace
   - Admin user: admin (or as specified in your .env file)
   - Admin password: (as specified in your .env file)
4. Under "Additional configurations":
   - Network and security: Choose your VPC and a security group
   - Database configurations: Set the port to 5439

### Cleaning Up Resources

To delete the Redshift cluster when you're done:

```bash
aws redshift delete-cluster --cluster-identifier rental-marketplace-redshift --skip-final-cluster-snapshot
```

## Getting Help

If you continue to experience issues:

1. Check the CloudWatch logs for detailed error messages
2. Review the AWS documentation for Redshift
3. Contact AWS Support if you have an AWS support plan
