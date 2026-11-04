"""Stream Kraken market data and compress its rotated archives."""


def main() -> None:
    """Launch the application through the installed console command.

    Args:
        None: Arguments are read from sys.argv by the application launcher.

    Returns:
        None: The application stopped following a user interruption.

    Raises:
        SystemExit: Startup or collection failed.
    """
    from streamingmarketdata.application import main as launch

    launch()
