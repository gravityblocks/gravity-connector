use gravity_protos::{
    auth::{
        GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role, Token,
        auth_service_client::AuthServiceClient,
    },
    block_engine::block_engine_validator_client::BlockEngineValidatorClient,
};
use solana_keypair::Keypair;
use solana_signer::Signer;
use tonic::{
    Request, Status,
    metadata::MetadataValue,
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, Endpoint},
};
use url::Url;

const CONN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub fn make_endpoint(url: &Url) -> Endpoint {
    let mut endpoint = Endpoint::from_shared(url.to_string()).expect("invalid url");
    if url.scheme() == "https" {
        endpoint = endpoint
            .tls_config(tonic::transport::ClientTlsConfig::new().with_enabled_roots())
            .unwrap();
    }
    endpoint
}

pub type BlockEngineConnectionResult<T> = Result<T, BlockEngineConnectionError>;

#[derive(Debug, thiserror::Error)]
pub enum BlockEngineConnectionError {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),

    #[error("connection timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("bad auth: {0}")]
    BadAuthenticationToken(&'static str),

    #[error("received invalid builder pubkey: {0}")]
    InvalidBuilderPubkey(String),
}

pub fn get_validated_token(maybe_token: Option<Token>) -> BlockEngineConnectionResult<Token> {
    let token = maybe_token.ok_or_else(|| {
        BlockEngineConnectionError::BadAuthenticationToken("received a null token")
    })?;
    if token.expires_at_utc.is_none() {
        Err(BlockEngineConnectionError::BadAuthenticationToken("expires_at_utc field is null"))
    } else {
        Ok(token)
    }
}

pub type JitoClient = BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>;

pub async fn create_client(
    endpoint: &Endpoint,
    identity_kp: &Keypair,
) -> Result<JitoClient, BlockEngineConnectionError> {
    let inner = tokio::time::timeout(CONN_TIMEOUT, endpoint.connect()).await??;
    let client_interceptor =
        AuthInterceptor::new(AuthServiceClient::new(inner), identity_kp, Role::Validator).await?;

    let inner = tokio::time::timeout(CONN_TIMEOUT, endpoint.connect()).await??;
    let client = BlockEngineValidatorClient::with_interceptor(inner, client_interceptor);

    Ok(client)
}

#[derive(Clone)]
pub struct AuthInterceptor {
    bearer_token: MetadataValue<tonic::metadata::Ascii>,
}

impl AuthInterceptor {
    pub async fn new(
        mut auth_service_client: AuthServiceClient<Channel>,
        keypair: &Keypair,
        role: Role,
    ) -> BlockEngineConnectionResult<Self> {
        let access_token = Self::auth(&mut auth_service_client, keypair, role).await?;
        let bearer_token: MetadataValue<tonic::metadata::Ascii> =
            format!("Bearer {}", access_token.value).parse().map_err(|_| {
                BlockEngineConnectionError::BadAuthenticationToken(
                    "token contains invalid header bytes",
                )
            })?;

        Ok(Self { bearer_token })
    }

    async fn auth(
        auth_service_client: &mut AuthServiceClient<Channel>,
        keypair: &Keypair,
        role: Role,
    ) -> BlockEngineConnectionResult<Token> {
        let challenge_resp = auth_service_client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: role as i32,
                pubkey: keypair.pubkey().as_ref().to_vec(),
            })
            .await?
            .into_inner();

        let formatted_challenge = format!("{}-{}", keypair.pubkey(), challenge_resp.challenge);
        let signed_challenge =
            keypair.sign_message(formatted_challenge.as_bytes()).as_ref().to_vec();

        let tokens = auth_service_client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge: formatted_challenge,
                client_pubkey: keypair.pubkey().as_ref().to_vec(),
                signed_challenge,
            })
            .await?
            .into_inner();

        let access_token = get_validated_token(tokens.access_token)?;
        Ok(access_token)
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = self.bearer_token.clone();
        request.metadata_mut().insert("authorization", token);
        Ok(request)
    }
}
