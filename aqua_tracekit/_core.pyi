"""Type stubs for aqua-tracekit._core (Rust extension module)."""

from typing import Any, Callable, Optional
from datetime import datetime
import polars as pl

class SdtModel:
    """Main model for aqua-tracekit with fishgroup segments, transfers, and containers."""
    
    def __init__(self, base_path: str) -> None:
        """Initialize model with base path for CSV files.
        
        Args:
            base_path: Directory path containing CSV files
        """
        ...
    
    # ── Data loading methods ──
    
    def load_csv(
        self,
        filename: str,
        rename: Optional[dict[str, str]] = None,
    ) -> pl.DataFrame:
        """Load any CSV into a Polars DataFrame with all columns as strings.
        
        Args:
            filename: CSV filename relative to base_path
            rename: Optional dictionary mapping old column names to new names
            
        Returns:
            DataFrame with all columns as strings
        """
        ...
    
    def load_transfers(self, filename: Optional[str] = None) -> pl.DataFrame:
        """Load transfers CSV.
        
        Required columns: source_pop_id, dest_pop_id
        Plus either stock columns (transfer_count, transfer_biomass_kg)
        or factor columns (share_count_forward, share_biomass_forward, 
                          share_count_backward, share_biomass_backward)
        
        Args:
            filename: CSV filename (default: "transfers.csv")
            
        Returns:
            DataFrame with transfers and calculated share factors
        """
        ...
    
    def load_containers(self, filename: Optional[str] = None) -> pl.DataFrame:
        """Load containers CSV.
        
        Required columns: container_id
        
        Args:
            filename: CSV filename (default: "containers.csv")
            
        Returns:
            DataFrame with containers
        """
        ...
    
    def load_populations(self, filename: Optional[str] = None) -> pl.DataFrame:
        """Load populations CSV.
        
        Required columns: population_id, container_id, start_time, end_time
        Datetime columns are parsed using format "%Y-%m-%d %H:%M:%S"
        
        Args:
            filename: CSV filename (default: "populations.csv")
            
        Returns:
            DataFrame with populations
        """
        ...
    
    def load_population_timeseries(self, filename: str) -> pl.DataFrame:
        """Load population-level timeseries CSV.
        
        Required columns: population_id, date_time
        
        Args:
            filename: CSV filename
            
        Returns:
            DataFrame with population timeseries
        """
        ...
    
    def load_container_timeseries(self, filename: str) -> pl.DataFrame:
        """Load container-level timeseries CSV.
        
        Required columns: container_id, date_time
        
        Args:
            filename: CSV filename
            
        Returns:
            DataFrame with container timeseries
        """
        ...
    
    # ── Parse helpers ──
    
    @staticmethod
    def parse_datetime(
        df: pl.DataFrame,
        column: str,
        format: str,
    ) -> pl.DataFrame:
        """Parse a string column to Datetime.
        
        Args:
            df: Input DataFrame
            column: Column name to parse
            format: Datetime format string (e.g., "%Y-%m-%d %H:%M:%S")
            
        Returns:
            DataFrame with parsed datetime column
        """
        ...
    
    @staticmethod
    def parse_float(df: pl.DataFrame, column: str) -> pl.DataFrame:
        """Parse a string column to Float64.
        
        Args:
            df: Input DataFrame
            column: Column name to parse
            
        Returns:
            DataFrame with parsed float column
        """
        ...
    
    @staticmethod
    def parse_int(df: pl.DataFrame, column: str) -> pl.DataFrame:
        """Parse a string column to Int64.
        
        Args:
            df: Input DataFrame
            column: Column name to parse
            
        Returns:
            DataFrame with parsed int column
        """
        ...
    
    # ── Tracing methods ──
    
    def trace_populations(self, origin_df: pl.DataFrame) -> pl.DataFrame:
        """Trace populations from a DataFrame containing population_id column.
        
        Args:
            origin_df: DataFrame with population_id column
            
        Returns:
            DataFrame with traceability index
        """
        ...
    
    # ── Filtering methods ──
    
    def get_populations_active_at(self, timestamp: datetime) -> pl.DataFrame:
        """Get populations active at a specific timestamp.
        
        Args:
            timestamp: Naive datetime (no timezone info)
            
        Returns:
            DataFrame with active populations
        """
        ...
    
    def get_populations_incoming(self) -> pl.DataFrame:
        """Get populations that have incoming transfers.
        
        Returns:
            DataFrame with populations
        """
        ...
    
    def get_populations_outgoing(self) -> pl.DataFrame:
        """Get populations that have outgoing transfers.
        
        Returns:
            DataFrame with populations
        """
        ...
    
    # ── Data joining methods ──
    
    @staticmethod
    def add_data_to_trace(
        pop_data: pl.DataFrame,
        traceability_index: pl.DataFrame,
    ) -> pl.DataFrame:
        """Merge traced population data with time-series or other population-level data.
        
        Args:
            pop_data: DataFrame with population data
            traceability_index: Traceability index DataFrame
            
        Returns:
            Merged DataFrame
        """
        ...
    
    def map_container_data_to_populations(
        self,
        container_data: pl.DataFrame,
        include_unmatched: bool =False, 
        allow_multiple: bool = False,
    ) -> pl.DataFrame:
        """Map container-level timeseries to populations.
        
        Joins on container_id and filters to each population's active period.
        
        Args:
            container_data: DataFrame with container_id and date_time columns
            allow_multiple: Whether to allow multiple population matches per input row
            
        Returns:
            DataFrame with container data mapped to populations
        """
        ...
    
    # ── Aggregation methods ──
    
    @staticmethod
    def aggregate_traced_data(
        traced_data: pl.DataFrame,
        aggregations: list[Aggregation],
        group_by: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """Aggregate traced data using built-in Rust aggregations.
        
        Args:
            traced_data: DataFrame with traced data
            aggregations: List of Aggregation objects
            group_by: Column names to group by (default: ["origin_population_id", "date_time"])
            
        Returns:
            Aggregated DataFrame
        """
        ...
    
    # ── Column mapping utility ──
    
    def map_column(
        self,
        df: pl.DataFrame,
        source_column: str,
        lookup_df: pl.DataFrame,
        lookup_key: str,
        lookup_value: str,
        new_column: Optional[str] = None,
    ) -> pl.DataFrame:
        """Map a column using a lookup table.
        
        Args:
            df: Input DataFrame
            source_column: Column in df to map
            lookup_df: Lookup table DataFrame
            lookup_key: Key column in lookup table
            lookup_value: Value column in lookup table
            new_column: Name for new column (default: lookup_value)
            
        Returns:
            DataFrame with mapped column
        """
        ...
    
    # ── Properties ──
    
    @property
    def transfers_df(self) -> Optional[pl.DataFrame]:
        """Get loaded transfers DataFrame."""
        ...
    
    @property
    def containers_df(self) -> Optional[pl.DataFrame]:
        """Get loaded containers DataFrame."""
        ...
    
    @property
    def populations_df(self) -> Optional[pl.DataFrame]:
        """Get loaded populations DataFrame."""
        ...
    
    # ── Visualization ──
    
    def visualize_trace(
        self,
        container_label_col: Optional[str] = None,
        population_label_col: Optional[str] = None,
        population_tooltip_cols: Optional[list[str]] = None,
        transfer_tooltip_cols: Optional[list[str]] = None,
        gap_px: int = 32,
        lane_height_px: int = 24,
        initial_zoom: float = 1.0,
    ) -> str:
        """Visualize the trace as an interactive timeline chart.
        
        Returns a self-contained HTML string with SVG and JS.
        Use with IPython.display.HTML(model.visualize_trace(...)) in Jupyter.
        
        Args:
            container_label_col: Column from containers df for y-axis labels (default: "container_id")
            population_label_col: Column from populations df to display on rectangles (default: "population_id")
            population_tooltip_cols: Columns from populations df to show on hover (default: [])
            transfer_tooltip_cols: Columns from transfers df to show on transfer hover
                                   (default: ["transfer_count", "transfer_biomass_kg"])
            gap_px: Pixel width of gap inserted at each transfer time (default: 32)
            lane_height_px: Pixel height per container lane (default: 24)
            initial_zoom: Initial zoom level (default: 1.0)
            
        Returns:
            HTML string with embedded SVG and JavaScript
        """
        ...


class Aggregation:
    """Declarative aggregation specification for traced data."""
    
    @staticmethod
    def custom(callable: Callable[[pl.DataFrame], dict[str, Any]]) -> Aggregation:
        """Create custom aggregation using a Python callable.
        
        Args:
            callable: Function that takes a DataFrame and returns a dict of aggregated values
            
        Returns:
            Aggregation object
        """
        ...
    
    @staticmethod
    def min(column: str, alias: Optional[str] = None) -> Aggregation:
        """Minimum value aggregation.
        
        Args:
            column: Column to aggregate
            alias: Output column name (default: "{column}_min")
            
        Returns:
            Aggregation object
        """
        ...
    
    @staticmethod
    def max(column: str, alias: Optional[str] = None) -> Aggregation:
        """Maximum value aggregation.
        
        Args:
            column: Column to aggregate
            alias: Output column name (default: "{column}_max")
            
        Returns:
            Aggregation object
        """
        ...
    
    @staticmethod
    def sum(columns: list[str]) -> Aggregation:
        """Sum aggregation for multiple columns.
        
        Args:
            columns: List of columns to sum
            
        Returns:
            Aggregation object
        """
        ...
    
    @staticmethod
    def avg(columns: list[str]) -> Aggregation:
        """Average aggregation for multiple columns.
        
        Args:
            columns: List of columns to average
            
        Returns:
            Aggregation object
        """
        ...
    
    @staticmethod
    def weighted_sum(
        columns: list[str],
        aggregate_by: str,  # "count" or "biomass"
        include_calculation: bool = False,
    ) -> Aggregation:
        """Direction-aware weighted sum aggregation.
        
        Automatically selects the correct factor columns based on trace direction:
        - For forward traces: uses backward factors (how much came from origin)
        - For backward traces: uses forward factors (how much went to descendants)
        - For identity: uses 1.0
        
        Args:
            columns: List of value columns to aggregate
            aggregate_by: Either "count" or "biomass" - determines which factor columns to use
            include_calculation: Whether to include calculation details
            
        Returns:
            Aggregation object
            
        Example:
            from aqua-tracekit.schema import SdtSchema
            Aggregation.weighted_sum(["mortality_count"], SdtSchema.AGGREGATE_BY.COUNT)
        """
        ...

    @staticmethod
    def weighted_avg(
        column: str, 
        aggregate_by: str) -> Aggregation:
        """Direction-aware weighted average aggregation.
        
        Automatically selects the correct factor columns based on trace direction:
        - For forward traces: uses forward factors (proper weights for averaging)
        - For backward traces: uses backward factors (proper weights for averaging)
        - For identity: uses 1.0
        
        Args:
            column: Value column to aggregate
            aggregate_by: Either "count" or "biomass" - determines which factor columns to use
            
        Returns:
            Aggregation object
            
        Example:
            from aqua-tracekit.schema import SdtSchema
            Aggregation.weighted_avg("temperature", SdtSchema.AGGREGATE_BY.BIOMASS)
        """
        ...
    
    @staticmethod
    def concat(
        columns: list[str],
        separator: str = ", ",
        unique: bool = False,
    ) -> Aggregation:
        """Concatenate column values.
        
        Args:
            columns: List of columns to concatenate
            separator: String separator (default: ", ")
            unique: Whether to deduplicate values (default: False)
            
        Returns:
            Aggregation object
        """
        ...
    
    @staticmethod
    def contribution_breakdown(
        columns: list[str],
        field_separator: str = ":",
        row_separator: str = ", ",
        alias: Optional[str] = None,
    ) -> Aggregation:
        """Create contribution breakdown showing all contributing rows.
        
        Args:
            columns: List of columns to include in breakdown
            field_separator: Separator between fields within a row (default: ":")
            row_separator: Separator between rows (default: ", ")
            alias: Output column name (default: "contribution_breakdown")
            
        Returns:
            Aggregation object
        """
        ...


# Schema submodules (constants exported from Rust)

class transfer:
    """Transfer column name constants."""
    SOURCE_POP_ID: str
    DEST_POP_ID: str
    TRANSFER_COUNT: str
    TRANSFER_BIOMASS_KG: str

class factors:
    """Trace factor column name constants."""
    SHARE_COUNT_FORWARD: str
    SHARE_BIOMASS_FORWARD: str
    SHARE_COUNT_BACKWARD: str
    SHARE_BIOMASS_BACKWARD: str

class direction:
    """Direction constants for traceability."""
    IDENTITY: str
    FORWARD: str
    BACKWARD: str

class population:
    """Population column name constants."""
    POPULATION_ID: str
    CONTAINER_ID: str
    START_TIME: str
    END_TIME: str

class container:
    """Container column name constants."""
    CONTAINER_ID: str

class traceability:
    """Traceability index column name constants."""
    ORIGIN_POPULATION_ID: str
    TRACED_POPULATION_ID: str
    TRACE_DIRECTION: str

class timeseries:
    """Time series column name constants."""
    DATE_TIME: str
