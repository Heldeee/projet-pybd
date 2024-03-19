# projet-pybd

## Analyzer

Les données sont les cours d'entreprises de différentes bourses relevées toutes les 10 mn. Un fihcier est un DataFrame stocké en mode pickle (pd.read_pickle) qui correspond à toutes les entreprises d'un marché à un instant donné par le titre du fichier.

## Dash

    d'afficher le cours d'une actions en échelle logarithmique avec une ligne ou avec des chandeliers (l'utilisateur pouvoir choisir). On doit pouvoir choisir le début et la fin de la période que l'on désire afficher.
    on doit pouvoir choisir plusieurs actions pour afficher leurs cours comme ci-dessus mais avec une couleur par action (pensez à la légende qui permet de désélectionner un cours)
    afficher des bandes de Bollinger pour un cours choisi
    afficher les données brutes dans un tableau pour les cours choisis avec en colonne le min, max, début, fin, moyenne, écart type pour chaque jour (une ligne = un jour).
    une fonctionnalité de votre choix