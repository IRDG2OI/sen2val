# sen2val
This code was produced as part of the G2OI project Co-financed by the European Union, the French State and the Region Réunion.

<div  style="float:left;">
    <img height=120  width=198  src="https://upload.wikimedia.org/wikipedia/commons/b/b7/Flag_of_Europe.svg">
    <img  src="https://upload.wikimedia.org/wikipedia/fr/thumb/2/22/Republique-francaise-logo.svg/512px-Republique-francaise-logo.svg.png?20201008150502"  height=120  width=140 >
    <img  height=120  width=260  src="https://upload.wikimedia.org/wikipedia/fr/3/3b/Logolareunion.png">
</div>
<br>

<p>Une des missions de l’UMR UMR Espace-Dev présente sur le site de la Station SEAS-OI consiste à traiter des images satellites afin d’en extraire de l’information. Cette dernière a notamment développé la chaîne de traitement Sen2Chain qui permet de télécharger et traiter des images de la constellation de satellites Sentinel 2 pour générer des indices radiométriques.</p>

</p>Dans le cadre du projet Grand Observatoire de l'Ocan Indien, l’action 1.2 est mise en œuvre par Corentin Souton, informaticien spécialisé en systèmes d’information géographique. La mission principale de cette action consiste à mettre en place des chaînes de traitements qui facilitent la publication de métadonnées et de données des partenaires du projet, en particulier des données de l'Observatoire des Sciences de l’Univers (OSU-R), de la station SEAS-OI (UMR Espace-Dev et service de valorisation des données de la Région Réunion) qui souhaitent mieux valoriser leurs données et les pérenniser.</p>

<p>Afin de valoriser ces productions, Corentin Souton a développé un script Python nommé “sen2val” dont l’objectif consiste à étendre la chaîne de traitement Sen2chain avec une étape de publication des métadonnées et des données. Ce script parcourt, sur le serveur de données Sentinel de SEAS-OI, les données produites pour extraire des métadonnées qui les décrivent et les stocker au standard Dublin Core (format de métadonnées également utilisé par la librairie geoflow). Ce script permettra ainsi la publication des métadonnées et des données dans l’infrastructure de données de la station SEAS-OI et dans des entrepôts de données qui attribuent des DOIs et répliquent la donnée. En pratique, le script “sen2val” réalisé par Corentin Souton intervient donc en aval de la chaîne Sen2Chain afin de réaliser les tâches suivantes :
    <ul>
        <li>générer un shapefile global sur l'emprise générale de la chaîne de traitement,</li>
        <li>générer un shapefile filtrant par indice,</li>
        <li>générer un shapefile filtrant par couple tuile-indice,</li>
        <li>créer des métadonnées conformes aux standards du Dublin Core utilisé par la librairie geoflow,</li>
        <li>automatiser la création de métadonnées au format OGC - ISO 19115 et la publication des métadonnées et des données dans l’IDS en appelant la bibliothèque geoflow</li>
        <li>exporter les données au format Netcdf afin de regrouper les images d’une même série temporelle dans un seul fichier qui pourra être déposé sur Thredds ou Zenodo (cf réunion SEAS-OI du 13/12/2023).</li>
    </ul>
</p>
