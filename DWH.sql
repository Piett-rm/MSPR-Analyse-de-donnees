CREATE TABLE Candidat(
   idCandidat INTEGER,
   sexe TEXT,
   nom TEXT,
   prenom TEXT,
   PRIMARY KEY(idCandidat)
);

CREATE TABLE Parti(
   idParti INTEGER,
   nomParti TEXT,
   couleur TEXT,
   PRIMARY KEY(idParti)
);

CREATE TABLE TypeCrime(
   idTypeCrime INTEGER,
   libelleTypeCrime TEXT,
   PRIMARY KEY(idTypeCrime)
);

CREATE TABLE Chomage(
   idChomage INTEGER,
   taux NUMERIC(5,2)  ,
   trimestre INTEGER,
   annee INTEGER,
   PRIMARY KEY(idChomage)
);

CREATE TABLE Richesse(
   idRichesse INTEGER,
   taux NUMERIC(3,2)  ,
   dateRichesse NUMERIC,
   PRIMARY KEY(idRichesse)
);

CREATE TABLE Circonscription(
   circonscription TEXT,
   idRichesse INTEGER NOT NULL,
   PRIMARY KEY(circonscription),
   FOREIGN KEY(idRichesse) REFERENCES Richesse(idRichesse)
);

CREATE TABLE TypeElection(
   idTypeElection INTEGER,
   libelleTypeElection TEXT,
   PRIMARY KEY(idTypeElection)
);

CREATE TABLE Election(
   idElection INTEGER,
   dateElection NUMERIC NOT NULL,
   libelleElection TEXT,
   tour TEXT,
   idTypeElection INTEGER NOT NULL,
   PRIMARY KEY(idElection),
   FOREIGN KEY(idTypeElection) REFERENCES TypeElection(idTypeElection)
);

CREATE TABLE Crime(
   idCrime INTEGER,
   anneeCrime INTEGER,
   nbCrime INTEGER,
   idTypeCrime INTEGER NOT NULL,
   PRIMARY KEY(idCrime),
   FOREIGN KEY(idTypeCrime) REFERENCES TypeCrime(idTypeCrime)
);

CREATE TABLE Departement(
   codeDepartement INTEGER,
   nomDepartement TEXT,
   idChomage INTEGER NOT NULL,
   PRIMARY KEY(codeDepartement),
   FOREIGN KEY(idChomage) REFERENCES Chomage(idChomage)
);

CREATE TABLE Commune(
   codeINSEE TEXT,
   libelleCommune TEXT,
   codePostal INTEGER,
   circonscription TEXT NOT NULL,
   codeDepartement INTEGER NOT NULL,
   idCrime INTEGER NOT NULL,
   PRIMARY KEY(codeINSEE),
   FOREIGN KEY(circonscription) REFERENCES Circonscription(circonscription),
   FOREIGN KEY(codeDepartement) REFERENCES Departement(codeDepartement),
   FOREIGN KEY(idCrime) REFERENCES Crime(idCrime)
);

CREATE TABLE Suffrage(
   idElection INTEGER,
   codeINSEE TEXT,
   nbVotant INTEGER,
   nbBlanc INTEGER,
   nbAbstention INTEGER,
   nbInscrit INTEGER,
   PRIMARY KEY(idElection, codeINSEE),
   FOREIGN KEY(idElection) REFERENCES Election(idElection),
   FOREIGN KEY(codeINSEE) REFERENCES Commune(codeINSEE)
);

CREATE TABLE Vote(
   idElection INTEGER,
   codeINSEE TEXT,
   idCandidat INTEGER,
   nbVote INTEGER,
   numeroPanneau INTEGER,
   PRIMARY KEY(idElection, codeINSEE, idCandidat),
   FOREIGN KEY(idElection) REFERENCES Election(idElection),
   FOREIGN KEY(codeINSEE) REFERENCES Commune(codeINSEE),
   FOREIGN KEY(idCandidat) REFERENCES Candidat(idCandidat)
);

CREATE TABLE Appartient(
   idCandidat INTEGER,
   idParti INTEGER,
   PRIMARY KEY(idCandidat, idParti),
   FOREIGN KEY(idCandidat) REFERENCES Candidat(idCandidat),
   FOREIGN KEY(idParti) REFERENCES Parti(idParti)
);
