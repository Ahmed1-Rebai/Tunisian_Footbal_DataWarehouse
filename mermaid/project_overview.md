graph TD
    A[Data Sources<br/>CSV files in data/] --> B[Extraction<br/>etl/extraction/]
    B --> C[Transformation<br/>etl/transformation/]
    C --> D[Loading<br/>etl/loading/]
    D --> E[Data Warehouse<br/>SQL Server]

    subgraph "Dimensions"
        D1[D_Team]
        D2[D_Competition]
        D3[D_Season]
        D4[D_Stadium]
        D5[D_Date]
        D6[D_Player]
    end

    subgraph "Facts"
        F1[F_Match]
        F2[F_Team_Season]
        F3[F_Team_Player_Season]
        F4[F_Champions]
        F5[F_TopScorers]
    end

    E --> D1
    E --> D2
    E --> D3
    E --> D4
    E --> D5
    E --> D6
    E --> F1
    E --> F2
    E --> F3
    E --> F4
    E --> F5

    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#e8f5e8
    style D fill:#fff3e0
    style E fill:#fce4ec
