from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Text, ForeignKey, BigInteger, CHAR, UniqueConstraint, Date
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class AuditMixin:
    """
    Mixin to add mandatory audit columns to every table.
    Ensures traceability of who/what created and updated records.
    """
    creat_by_nm = Column(String(100), nullable=False, default='DAGSTER')
    creat_dttm = Column(DateTime, nullable=False, default=datetime.utcnow)
    updt_by_nm = Column(String(100))
    updt_dttm = Column(DateTime, onupdate=datetime.utcnow)

class ETLOrg(Base, AuditMixin):
    """
    Organization (Tenant) Registry.
    The primary isolation boundary for SaaS multi-tenancy.
    """
    __tablename__ = "etl_org"
    id = Column(Integer, primary_key=True)
    org_nm = Column(String(100), unique=True, nullable=False) # e.g. "Enterprise Data Services"
    org_code = Column(String(20), unique=True, nullable=False)  # e.g. "EDS"
    description = Column(String(255))
    actv_ind = Column(Boolean, default=True)

    # Relationships
    users = relationship("ETLUser", back_populates="org")
    teams = relationship("ETLTeam", back_populates="org")
    jobs = relationship("ETLJob", back_populates="org")
    job_definitions = relationship("ETLJobDefinition", back_populates="org")
    blueprints = relationship("ETLBlueprint", back_populates="org")
    job_instances = relationship("ETLJobInstance", back_populates="org")
    connections = relationship("ETLConnection", back_populates="org")
    job_statuses = relationship("ETLJobStatus", back_populates="org")

class ETLTeam(Base, AuditMixin):
    """
    Team (Business Unit) Registry.
    Scoped within an Organization.
    """
    __tablename__ = "etl_team"
    id = Column(Integer, primary_key=True)
    org_id = Column(Integer, ForeignKey("etl_org.id"), nullable=False)
    team_nm = Column(String(100), nullable=False)
    description = Column(String(255))
    actv_ind = Column(Boolean, default=True)

    # Relationships
    org = relationship("ETLOrg", back_populates="teams")
    code_locations = relationship("ETLCodeLocation", back_populates="team")
    jobs = relationship("ETLJob", back_populates="team")
    job_definitions = relationship("ETLJobDefinition", back_populates="team")
    blueprints = relationship("ETLBlueprint", back_populates="team")
    job_instances = relationship("ETLJobInstance", back_populates="team")
    connections = relationship("ETLConnection", back_populates="team")
    job_statuses = relationship("ETLJobStatus", back_populates="team")
    roles = relationship("ETLRole", back_populates="team")
    memberships = relationship("ETLTeamMember", back_populates="team")

    __table_args__ = (
        UniqueConstraint("org_id", "team_nm", name="uq_org_team"),
    )

class ETLCodeLocation(Base, AuditMixin):
    """
    Code Location (Repository) Registry.
    A specific project/repo scoped within a Team.
    """
    __tablename__ = "etl_code_location"
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("etl_team.id"), nullable=False)
    location_nm = Column(String(100), nullable=False)
    repo_url = Column(String(255))
    
    # Relationships
    team = relationship("ETLTeam", back_populates="code_locations")
    jobs = relationship("ETLJob", back_populates="code_location")
    job_definitions = relationship("ETLJobDefinition", back_populates="code_location")
    job_instances = relationship("ETLJobInstance", back_populates="code_location")

    __table_args__ = (
        UniqueConstraint("team_id", "location_nm", name="uq_team_location"),
    )

class ETLConnection(Base, AuditMixin):
    __tablename__ = "etl_connection"
    
    id = Column(Integer, primary_key=True)
    conn_nm = Column(String(255), unique=True, nullable=False)
    conn_type = Column(String(50), nullable=False)
    config_json = Column(JSONB, nullable=False, default={})
    
    # Scoping for Service Account Isolation
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    owner_type = Column(String(20)) # 'TEAM', 'CODE_LOC'
    owner_id = Column(Integer)
    
    # Relationships
    org = relationship("ETLOrg", back_populates="connections")
    team = relationship("ETLTeam", back_populates="connections")
    # Removing legacy created_at in favor of AuditMixin

class ETLSchedule(Base, AuditMixin):
    __tablename__ = "etl_schedule"
    
    id = Column(Integer, primary_key=True)
    org_id = Column(Integer, ForeignKey("etl_org.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    slug = Column(String(255), unique=True, nullable=False)
    cron = Column(String(100), nullable=False)
    timezone = Column(String(100))
    actv_ind = Column(Boolean, default=True)
    
    # Relationships
    org = relationship("ETLOrg")
    team = relationship("ETLTeam")
    # Removing legacy created_at in favor of AuditMixin

class ETLJob(Base, AuditMixin):
    __tablename__ = "etl_job"
    
    id = Column(Integer, primary_key=True)
    job_nm = Column(String(255), nullable=False)
    invok_id = Column(String(255), nullable=False)
    
    # Scoping
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"))
    
    # Relationships
    org = relationship("ETLOrg", back_populates="jobs")
    team = relationship("ETLTeam", back_populates="jobs")
    code_location = relationship("ETLCodeLocation", back_populates="jobs")
    
    schedule_id = Column(Integer, ForeignKey("etl_schedule.id")) # Linked to centralized schedule
    cron_schedule = Column(String(100)) # Legacy, will be deprecated
    partition_start_dt = Column(Date)
    actv_ind = Column(Boolean, default=True)
    # Removing legacy created_at in favor of AuditMixin
    
    __table_args__ = (
        UniqueConstraint("job_nm", "invok_id", name="uq_job_invok"),
        {"sqlite_autoincrement": True}, # For testing if needed
    )

class ETLJobDefinition(Base, AuditMixin):
    """
    Source Registry (The 'Template').
    Stores the serialized YAML definition from disk.
    """
    __tablename__ = "etl_job_definition"
    
    id = Column(Integer, primary_key=True)
    job_nm = Column(String(255), nullable=False)
    description = Column(String(255))
    
    # Serialized Data (Phase 15 Expansion)
    file_loc = Column(String(512)) # Path relative to base_dir
    file_hash = Column(String(64)) # MD5 Hash of file content
    yaml_content = Column(Text)    # Raw YAML string
    yaml_def = Column(JSONB)       # Parsed JSON representation
    params_schema = Column(JSONB)  # JSON Schema for parameters
    asset_selection = Column(JSONB) # List of assets
    
    # Scoping
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"))
    actv_ind = Column(Boolean, default=True)

    # Relationships
    org = relationship("ETLOrg", back_populates="job_definitions")
    team = relationship("ETLTeam", back_populates="job_definitions")
    code_location = relationship("ETLCodeLocation", back_populates="job_definitions")
    instances = relationship("ETLJobInstance", back_populates="definition")

    __table_args__ = (
        UniqueConstraint("job_nm", "team_id", "code_location_id", name="uq_job_team_loc_def"),
    )

class ETLBlueprint(Base, AuditMixin):
    """
    Blueprint Registry (The 'Logic Template').
    Stores logic-only YAMLs marked with blueprint: true.
    """
    __tablename__ = "etl_blueprint"
    
    id = Column(Integer, primary_key=True)
    blueprint_nm = Column(String(255), nullable=False)
    description = Column(String(255))
    
    # Serialized Data
    file_loc = Column(String(512)) # Path relative to base_dir
    file_hash = Column(String(64)) # MD5 Hash of file content
    yaml_content = Column(Text)    # Raw YAML string
    yaml_def = Column(JSONB)       # Parsed JSON representation (Logic only)
    params_schema = Column(JSONB)  # JSON Schema for parameters
    asset_selection = Column(JSONB) # List of assets
    
    # Scoping
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"))
    actv_ind = Column(Boolean, default=True)

    # Relationships
    org = relationship("ETLOrg", back_populates="blueprints")
    team = relationship("ETLTeam", back_populates="blueprints")
    code_location = relationship("ETLCodeLocation")
    instances = relationship("ETLJobInstance", back_populates="blueprint")

    __table_args__ = (
        UniqueConstraint("blueprint_nm", "team_id", "code_location_id", name="uq_blueprint_team_loc"),
    )

class ETLJobInstance(Base, AuditMixin):
    """
    Deployment Registry (The 'Actual Pipeline').
    Stores specific runtime configurations and schedules linked to a definition.
    """
    __tablename__ = "etl_job_instance"
    
    id = Column(Integer, primary_key=True)
    instance_id = Column(String(255), nullable=False) # Maps to invok_id in old logic
    description = Column(String(255))
    
    # Links
    job_definition_id = Column(Integer, ForeignKey("etl_job_definition.id")) # Legacy/Static
    blueprint_id = Column(Integer, ForeignKey("etl_blueprint.id"))         # Modern Blueprint
    schedule_id = Column(Integer, ForeignKey("etl_schedule.id"))
    
    # Overrides
    cron_schedule = Column(String(100))
    partition_start_dt = Column(Date)
    actv_ind = Column(Boolean, default=True)

    # Relationships
    definition = relationship("ETLJobDefinition", back_populates="instances")
    blueprint = relationship("ETLBlueprint", back_populates="instances")
    org = relationship("ETLOrg", back_populates="job_instances")
    team = relationship("ETLTeam", back_populates="job_instances")

    # Scoping (Denormalized for easy lookup)
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))

    __table_args__ = (
        UniqueConstraint("instance_id", "job_definition_id", name="uq_inst_def"),
    )

class ETLJobParameter(Base, AuditMixin):
    __tablename__ = "etl_job_parameter"
    
    id = Column(Integer, primary_key=True)
    etl_job_id = Column(Integer, ForeignKey("etl_job.id", ondelete="CASCADE"), unique=True)
    config_json = Column(JSONB, nullable=False, default={})
    # Removing legacy updated_at in favor of AuditMixin

class ETLParameter(Base, AuditMixin):
    __tablename__ = "etl_parameter"
    
    id = Column(Integer, primary_key=True)
    parm_nm = Column(String(255), unique=True, nullable=False)
    parm_value = Column(Text)
    # Removing legacy created_at in favor of AuditMixin

class ETLParamsSchema(Base, AuditMixin):
    """
    Developer Contract Table.
    Stores the expected parameter schema for a job name.
    """
    __tablename__ = "etl_params_schema"
    
    id = Column(Integer, primary_key=True)
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"))
    job_nm = Column(String(255), nullable=False)
    json_schema = Column(JSONB, nullable=False)
    description = Column(Text)
    is_strict = Column(Boolean, default=False)

    # Relationships
    org = relationship("ETLOrg")
    team = relationship("ETLTeam")
    code_location = relationship("ETLCodeLocation")

    __table_args__ = (
        UniqueConstraint("job_nm", "team_id", "code_location_id", name="uq_job_team_location_schema"),
    )

class ETLConnTypeSchema(Base, AuditMixin):
    """
    Connection Type Schema Registry.
    Stores the expected configuration schema for each connection type (S3, SFTP, etc).
    """
    __tablename__ = "etl_conn_type_schema"
    
    id = Column(Integer, primary_key=True)
    conn_type = Column(String(50), unique=True, nullable=False)
    json_schema = Column(JSONB, nullable=False)
    description = Column(Text)

class ETLJobStatus(Base, AuditMixin):
    __tablename__ = "etl_job_status"
    
    btch_nbr = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(64), unique=True, nullable=False)
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    job_nm = Column(String(256), nullable=False)
    invok_id = Column(String(255))
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    btch_sts_cd = Column(CHAR(1), default='R') # R, C, A
    run_mde_txt = Column(String(50), nullable=False) # SCHEDULED, MANUAL, BACKFILL
    
    # Relationships
    org = relationship("ETLOrg", back_populates="job_statuses")
    team = relationship("ETLTeam", back_populates="job_statuses")
    # Audit columns are now provided by Mixin

class ETLAssetStatus(Base, AuditMixin):
    __tablename__ = "etl_asset_status"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    btch_nbr = Column(BigInteger, ForeignKey("etl_job_status.btch_nbr", ondelete="CASCADE"), nullable=False)
    asset_nm = Column(String(256), nullable=False)
    parent_assets = Column(JSONB) # List of upstream asset names
    config_json = Column(JSONB)
    partition_key = Column(String(255))
    dagster_event_type = Column(String(50)) # Materialization, Check, etc.
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    asset_sts_cd = Column(CHAR(1), default='R') # R, C, A
    err_msg_txt = Column(Text)
    # Audit columns are now provided by Mixin

class ETLRole(Base, AuditMixin):
    """
    Role Registry.
    Defines the permission tiers: DPE_PLATFORM_ADMIN, DPE_DEVELOPER, DPE_DATA_ANALYST.
    """
    __tablename__ = "etl_role"
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("etl_team.id")) # Scoped to a team, or NULL for global/org roles
    role_nm = Column(String(100), nullable=False)
    description = Column(String(255))
    actv_ind = Column(Boolean, default=True)

    # Scoped roles should be unique per team
    __table_args__ = (
        UniqueConstraint("team_id", "role_nm", name="uq_team_role"),
    )

    # Relationships
    team = relationship("ETLTeam", back_populates="roles")
    users = relationship("ETLUser", secondary="etl_team_member", back_populates="team_roles")

class ETLUser(Base, AuditMixin):
    """
    User Registry.
    Stores identities and secure credential hashes.
    """
    __tablename__ = "etl_user"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)  # Salted bcrypt hash
    full_nm = Column(String(255), nullable=False)
    email = Column(String(255))
    
    # FKs
    role_id = Column(Integer, ForeignKey("etl_role.id"), nullable=False)
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    default_team_id = Column(Integer, ForeignKey("etl_team.id"))
    
    actv_ind = Column(Boolean, default=True)

    # Relationships
    role = relationship("ETLRole", back_populates="users", overlaps="team_roles")
    org = relationship("ETLOrg", back_populates="users")
    team_memberships = relationship("ETLTeamMember", back_populates="user")
    team_roles = relationship("ETLRole", secondary="etl_team_member", back_populates="users", overlaps="role")

class ETLTeamMember(Base, AuditMixin):
    """
    Join table for User-Team-Role mapping.
    Enables a user to have different roles in different teams.
    """
    __tablename__ = "etl_team_member"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("etl_user.id", ondelete="CASCADE"), nullable=False)
    team_id = Column(Integer, ForeignKey("etl_team.id", ondelete="CASCADE"), nullable=False)
    role_id = Column(Integer, ForeignKey("etl_role.id"), nullable=False)
    
    actv_ind = Column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint("user_id", "team_id", name="uq_user_team"),
    )

    # Relationships
    user = relationship("ETLUser", back_populates="team_memberships")
    team = relationship("ETLTeam", back_populates="memberships")
    role = relationship("ETLRole")
