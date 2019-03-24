# XKE UDAF

## Les UDAFs en Spark

## Les exercices

Les exercices sont dans le package `fr.xebia.udaf`. Il y a ensuite un package par exercice. Pour chaque UDAF les 8 méthodes sont pré-écrites. Vous n'avez plus qu'à les implémenter.

#### Exemple - Moyenne
Dans le package `example` vous avez un exemple de l'implémentation du calcul d'une moyenne avec une UDAF. 

#### Exercice 1 - Maximum
Implémentez une UDAF qui calcule le maximum d'une colonne.

#### Exercice 2 - Moyenne Géométrique
Implémentez une UDAF qui calcule la moyenne géométrique d'une colonne. Pour une série `x1, ... , xn` de nombres, la moyenne géométrique se calcul de la façon suivante :  
 <img src="https://latex.codecogs.com/svg.latex?\sqrt[n]{\prod_{i=1}^{n}x_i}" />

#### Exercice 3 - Ecart type
Implémentez une UDAF qui calcule l'écart type d'une colonne. Pour une série `x1, ... , xn` de nombres, l'écart type se calcul de la façon suivante :  
 <img src="https://latex.codecogs.com/svg.latex?\sqrt{\frac{1}{n-1}\sum_{i=1}^{n}{(x_i-\mu)^2}}" />

Où *&mu;* est la moyenne de la série

#### Bonus 1 - Skewness
Implémentez une UDAF qui calcule le skewness (l'asymétrie) d'une colonne. Pour une série `x1, ... , xn` de nombres, le skewness se calcul de la façon suivante :  
 <img src="https://latex.codecogs.com/svg.latex?\sqrt{n}\frac{\sum_{i=1}^{n}(x_i-\mu)^3}{(\sum_{i=1}^{n}(x_i-\mu)^2)^{\frac{3}{2}}}" />

Où *&mu;* est la moyenne de la série

#### Bonus 2 - Kurtosis
Implémentez une UDAF qui calcule le kurtosis (l'aplatissement) d'une colonne. Pour une série `x1, ... , xn` de nombres, le kurtosis se calcul de la façon suivante :  
 <img src="https://latex.codecogs.com/svg.latex?n\frac{\sum_{i=1}^{n}(x_i-\mu)^4}{(\sum_{i=1}^{n}(x_i-\mu)^2)^2}-3" />

Où *&mu;* est la moyenne de la série
