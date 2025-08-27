import typer
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker, Session
from rich.console import Console
from rich.table import Table

from app.db import db_manager
from app.models import Lead, ETAInference, Candidate

app = typer.Typer()
console = Console()

def get_session() -> Session:
    return db_manager.get_session_sync()

@app.command()
def run(
    days_past: int = typer.Option(30, "--days-past", "-d", help="How many days in the past to look for predicted ETAs."),
    auto_close_unopened: bool = typer.Option(False, "--auto-close", help="Automatically mark leads older than 120 days as 'Did not open'.")
):
    """Run the feedback loop to update lead outcomes and measure ETA accuracy."""
    console.print(f"[bold cyan]🚀 Starting Feedback Loop...[/bold cyan]")
    
    session = get_session()
    
    try:
        leads_to_review = _get_leads_for_review(session, days_past)
        
        if not leads_to_review:
            console.print("[yellow]No leads found for review in the specified timeframe.[/yellow]")
            return

        _process_leads(session, leads_to_review)

        if auto_close_unopened:
            _handle_auto_close(session)

        _print_accuracy_summary(session)

    finally:
        session.close()

def _get_leads_for_review(session: Session, days_past: int) -> list:
    """Get leads with past predicted ETAs that have no feedback yet."""
    review_cutoff_date = datetime.utcnow() - timedelta(days=days_past)
    
    return (
        session.query(Lead)
        .join(Candidate)
        .join(ETAInference)
        .filter(Lead.feedback_status == None)
        .filter(ETAInference.eta_end < review_cutoff_date)
        .order_by(ETAInference.eta_end.asc())
        .all()
    )

def _process_leads(session: Session, leads: list):
    """Interactively process each lead for feedback."""
    for lead in leads:
        console.print("\n" + "-"*50)
        _display_lead_info(lead)
        
        actual_date_str = typer.prompt("Enter actual opening date (YYYY-MM-DD), or press Enter to skip", default="").strip()
        
        if not actual_date_str:
            continue

        try:
            actual_date = datetime.strptime(actual_date_str, "%Y-%m-%d")
            lead.actual_opening_date = actual_date
            
            feedback_options = ["Opened on time", "Opened early", "Opened late", "Did not open", "Other"]
            feedback_status = typer.prompt(
                "Feedback status?", 
                type=click.Choice(feedback_options, case_sensitive=False),
                default="Opened on time"
            )
            lead.feedback_status = feedback_status

            if feedback_status == "Other":
                lead.feedback_notes = typer.prompt("Notes")

            session.add(lead)
            session.commit()
            console.print("[green]✔ Feedback saved.[/green]")

        except ValueError:
            console.print("[red]Invalid date format. Skipping.[/red]")

def _display_lead_info(lead: Lead):
    """Display information about the lead being reviewed."""
    table = Table(title=f"[bold]Reviewing Lead: {lead.candidate.venue_name}[/bold]")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="magenta")

    predicted_eta = lead.candidate.eta_inferences[-1] if lead.candidate.eta_inferences else None

    table.add_row("Address", lead.candidate.address)
    if predicted_eta:
        table.add_row("Predicted Opening", f"{predicted_eta.eta_start.date()} to {predicted_eta.eta_end.date()}")
        table.add_row("Prediction Confidence", f"{predicted_eta.confidence_0_1:.2f}")

    console.print(table)

def _handle_auto_close(session: Session):
    """Automatically close very old, un-actioned leads."""
    auto_close_cutoff = datetime.utcnow() - timedelta(days=120)
    
    closed_count = (
        session.query(Lead)
        .join(ETAInference, Lead.candidate_id == ETAInference.candidate_id)
        .filter(Lead.feedback_status == None)
        .filter(ETAInference.eta_end < auto_close_cutoff)
        .update({"feedback_status": "Did not open", "feedback_notes": "Auto-closed due to age."})
    )
    
    if closed_count > 0:
        session.commit()
        console.print(f"[blue]Auto-closed {closed_count} very old leads.[/blue]")

def _print_accuracy_summary(session: Session):
    """Print a summary of prediction accuracy."""
    reviewed_leads = session.query(Lead).filter(Lead.actual_opening_date != None).all()
    
    if not reviewed_leads:
        console.print("\n[yellow]No reviewed leads with actual opening dates to analyze.[/yellow]")
        return

    total_error_days = 0
    total_leads = len(reviewed_leads)
    
    for lead in reviewed_leads:
        predicted_eta = lead.candidate.eta_inferences[-1]
        predicted_mid_date = predicted_eta.eta_start + (predicted_eta.eta_end - predicted_eta.eta_start) / 2
        error_days = abs((lead.actual_opening_date - predicted_mid_date).days)
        total_error_days += error_days

    avg_error = total_error_days / total_leads

    console.print("\n[bold underline]Prediction Accuracy Summary[/bold underline]")
    console.print(f"Total Leads Analyzed: {total_leads}")
    console.print(f"Average Prediction Error: [bold]{avg_error:.2f} days[/bold]")

if __name__ == "__main__":
    app()
