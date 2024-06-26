from src.db.players import update_or_insert_players
from src.db.output import save_output
from src.data.load import load_data
from src.spark.context import get_context
from src.data.columns import events_cols as cols


def yellow_red_cards_players():

    sc = get_context("yellow/red cards players")

    events = load_data(sc, "events.csv")

    cards = events.filter(lambda x: x[cols["event_type"]] in ["4", "5", "6"])

    # mapping to (player, (yellow cards, red cards))
    cards = cards.map(lambda x: (
        x[cols["player"]],
        (
            1*(x[cols["event_type"]] in ["4", "5"]),
            1*(x[cols["event_type"]] in ["5", "6"])
        )
    ))

    cards = cards.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    cards_list = cards.collect()

    cards_dicts = [
        {"name": x[0], "yellow cards": x[1][0], "red cards": x[1][1]} for x in cards_list
    ]

    update_or_insert_players(cards_dicts)

    # Retrieve the top teams by best average goals per game
    top_players = cards.takeOrdered(10, key=lambda x: -x[1][1])

    save_output(top_players, lambda x: f"{x[0]}: {x[1][0]} Yellow cards, {x[1][1]} Red cards",
                "Top 10 players by number of red cards")

    sc.stop()
