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
    connections = relationship("ETLConnection", back_populates="org")
    job_definitions = relationship("ETLJobDefinition", back_populates="org")
    job_instances = relationship("ETLJobInstance", back_populates="org")
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
    connections = relationship("ETLConnection", back_populates="team")
    job_definitions = relationship("ETLJobDefinition", back_populates="team")
    job_instances = relationship("ETLJobInstance", back_populates="team")
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
    job_definitions = relationship("ETLJobDefinition", back_populates="code_location")
    templates = relationship("ETLJobTemplate", back_populates="code_location")
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

class ETLJobTemplate(Base, AuditMixin):
    """
    Job Template (The "Blueprint").
    Stores reusable patterns identified by 'template: true' in YAML.
    """
    __tablename__ = "etl_job_template"

    id = Column(Integer, primary_key=True)
    template_nm = Column(String(255), nullable=False)
    description = Column(String(1000))
    yaml_def = Column(JSONB)  # The blueprint configuration
    params_schema = Column(JSONB)  # The contract for users
    asset_selection = Column(JSONB) # The assets this template provides
    
    # Scoping
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"))
    
    actv_ind = Column(Boolean, default=True)

    # Relationships
    org = relationship("ETLOrg")
    team = relationship("ETLTeam")
    code_location = relationship("ETLCodeLocation", back_populates="templates")
    definitions = relationship("ETLJobDefinition", back_populates="template")
    instances = relationship("ETLJobInstance", back_populates="template")

    __table_args__ = (
        UniqueConstraint("template_nm", "team_id", "code_location_id", name="uq_job_template_team_location"),
    )

class ETLJobDefinition(Base, AuditMixin):
    """
    Job Definition (The "Executable Pipeline").
    Stores YAML-defined singleton jobs.
    """
    __tablename__ = "etl_job_definition"
    
    id = Column(Integer, primary_key=True)
    job_nm = Column(String(255), nullable=False)
    description = Column(String(1000))
    
    # Logic Source
    template_id = Column(Integer, ForeignKey("etl_job_template.id"))
    is_singleton = Column(Boolean, default=True)
    
    # State Configuration
    yaml_def = Column(JSONB)  # The YAML content
    params_schema = Column(JSONB) # Formal schema
    asset_selection = Column(JSONB) # Final selection
    
    # Scheduling Info for Singletons
    schedule_id = Column(Integer, ForeignKey("etl_schedule.id"))
    cron_schedule = Column(String(100))
    partition_start_dt = Column(Date)
    
    # Scoping
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"))
    
    actv_ind = Column(Boolean, default=True)

    # Relationships
    org = relationship("ETLOrg", back_populates="job_definitions")
    team = relationship("ETLTeam", back_populates="job_definitions")
    code_location = relationship("ETLCodeLocation", back_populates="job_definitions")
    template = relationship("ETLJobTemplate", back_populates="definitions")
    schedule = relationship("ETLSchedule")
    parameters = relationship("ETLJobParameter", back_populates="job_definition")
    job_statuses = relationship("ETLJobStatus", back_populates="job_definition")

    __table_args__ = (
        UniqueConstraint("job_nm", "team_id", "code_location_id", name="uq_job_def_team_location"),
    )

class ETLJobInstance(Base, AuditMixin):
    """
    Phase 18: Job Instance (UI-Created Patterns).
    Stores registrations made via the Dashboard from templates.
    """
    __tablename__ = "etl_job_instance"
    
    id = Column(Integer, primary_key=True)
    instance_nm = Column(String(255), nullable=False)
    description = Column(String(1000))
    
    template_id = Column(Integer, ForeignKey("etl_job_template.id"), nullable=False)
    params_json = Column(JSONB) # Runtime parameter values
    
    # Scheduling
    cron_schedule = Column(String(100))
    partition_start_dt = Column(Date)
    
    # Scoping
    org_id = Column(Integer, ForeignKey("etl_org.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("etl_team.id"), nullable=False)
    code_location_id = Column(Integer, ForeignKey("etl_code_location.id"), nullable=False)
    
    actv_ind = Column(Boolean, default=True)

    # Relationships
    template = relationship("ETLJobTemplate", back_populates="instances")
    org = relationship("ETLOrg", back_populates="job_instances")
    team = relationship("ETLTeam", back_populates="job_instances")
    code_location = relationship("ETLCodeLocation", back_populates="job_instances")
    job_statuses = relationship("ETLJobStatus", back_populates="job_instance")

    __table_args__ = (
        UniqueConstraint("instance_nm", "team_id", "code_location_id", name="uq_job_instance_name_team_location"),
    )

class ETLJobParameter(Base, AuditMixin):
    """
    Runtime Parameters for an Executable Job (Legacy/Singleton).
    """
    __tablename__ = "etl_job_parameter"
    
    id = Column(Integer, primary_key=True)
    job_definition_id = Column(Integer, ForeignKey("etl_job_definition.id", ondelete="CASCADE"), unique=True)
    config_json = Column(JSONB, nullable=False, default={})

    # Relationships
    job_definition = relationship("ETLJobDefinition", back_populates="parameters")

class ETLParameter(Base, AuditMixin):
    """
    Global/Enterprise Parameters.
    """
    __tablename__ = "etl_parameter"
    
    id = Column(Integer, primary_key=True)
    parm_nm = Column(String(255), unique=True, nullable=False)
    parm_value = Column(Text)

class ETLConnTypeSchema(Base, AuditMixin):
    """
    Connection Type Schema Registry.
    """
    __tablename__ = "etl_conn_type_schema"
    
    id = Column(Integer, primary_key=True)
    conn_type = Column(String(50), unique=True, nullable=False)
    json_schema = Column(JSONB, nullable=False)
    description = Column(Text)

class ETLJobStatus(Base, AuditMixin):
    """
    Execution History (Run Registry).
    """
    __tablename__ = "etl_job_status"
    
    btch_nbr = Column(BigInteger, primary_key=True, autoincrement=True)
    run_id = Column(String(64), unique=True, nullable=False)
    org_id = Column(Integer, ForeignKey("etl_org.id"))
    team_id = Column(Integer, ForeignKey("etl_team.id"))
    
    job_definition_id = Column(Integer, ForeignKey("etl_job_definition.id"))
    job_instance_id = Column(Integer, ForeignKey("etl_job_instance.id"))
    job_nm = Column(String(256), nullable=False) # Copied for convenience
    
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    btch_sts_cd = Column(CHAR(1), default='R') # R, C, A
    run_mde_txt = Column(String(50), nullable=False) # SCHEDULED, MANUAL, BACKFILL
    
    # Relationships
    org = relationship("ETLOrg", back_populates="job_statuses")
    team = relationship("ETLTeam", back_populates="job_statuses")
    job_definition = relationship("ETLJobDefinition", back_populates="job_statuses")
    job_instance = relationship("ETLJobInstance", back_populates="job_statuses")

class ETLAssetStatus(Base, AuditMixin):
    """
    Step-Level Execution Details.
    """
    __tablename__ = "etl_asset_status"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    btch_nbr = Column(BigInteger, ForeignKey("etl_job_status.btch_nbr", ondelete="CASCADE"), nullable=False)
    asset_nm = Column(String(256), nullable=False)
    parent_assets = Column(JSONB) 
    config_json = Column(JSONB)
    partition_key = Column(String(255))
    dagster_event_type = Column(String(50)) 
    strt_dttm = Column(DateTime, default=datetime.utcnow)
    end_dttm = Column(DateTime)
    asset_sts_cd = Column(CHAR(1), default='R') 
    err_msg_txt = Column(Text)

class ETLRole(Base, AuditMixin):
    """
    RBAC Role Registry.
    """
    __tablename__ = "etl_role"
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("etl_team.id")) 
    role_nm = Column(String(100), nullable=False)
    description = Column(String(255))
    actv_ind = Column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint("team_id", "role_nm", name="uq_team_role"),
    )

    # Relationships
    team = relationship("ETLTeam", back_populates="roles")
    users = relationship("ETLUser", secondary="etl_team_member", back_populates="team_roles")

class ETLUser(Base, AuditMixin):
    """
    User Registry.
    """
    __tablename__ = "etl_user"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
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
    User-Team Membership.
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
