import random

class Saulisms(object):
    sayings = []

    def __init__(self):
        self.sayings = [
            ("I am a very respectful person. -Saul Alarcon", "2017"),
            ("You have to give them a little bit of food, so that they get hungry. -Saul Alarcon", "2/19/2019"),
            ("You are the God, please help me. -Saul Alarcon", "3/12/2019"),
            ("When I write my letters, I always use \"Respectfully, ...\", not \"Sincerely, ...\" because when I write the word, I feel that's who I am. The word has more weight. -Saul Alarcon", "4/11/2019"),
            ("There is one letter that is key in life, the letter O. O for opportunity. -Saul Alarcon", "4/23/2019"),
            ("I am just going to ask you 1 thing... -Saul Alarcon", "2019"),
            ("The shovel is the best sensor we can have....and by the way, its wireless ;) -Saul Alarcon", "2019"),
            ("Soy tan cabron que agarre el mosco del aire, soy mas rapido que el rayo! -Saul Alarcon", "5/24/2019"),
            ("Hay una tumba que dice, \"Les dije que me sentía mal culeros\". -Saul Alarcon", "6/3/2019"),
            ("If I could be a plant, I would be a tomato plant. -Saul Alarcon", "7/30/2019"),
            ("Yo tambien era un perro sin negocio, en mi tiempo, sin oficio ni beneficio. -Saul Alarcon", "10/29/2019"),
            ("Yo creci en la calle eh... cuidado XD -Saul Alarcon", "11/19/2019"),
            ("Si alguien me está tratando de quitar el hueso, cuando yo veo eso me pongo más cabron. -Saul Alarcon", "1/23/2020"),
            ("I wish tomato plants were women because I can talk to them better. And her name would be BQ-273. -Saul Alarcon", "3/3/2020"),
            ("One thing that I always call it is, \"the power of the contract\". Who has the power of the contract? BD has the power. -Saul Alarcon", "10/28/2020"),
            ("Si no quieren trabajar con nosotros para el próximo año, se los va a cargar la chingada. -Saul Alarcon", "11/19/2020"),
            ("If there is no mission, there is no ambition! -Saul Alarcon", "8/25/2021"),
            ("Yo me avente la pinche tesis con dos dedos! -Saul Alarcon", "4/27/2022"),
            ("Yo no tengo problemas, tengo soluciones. -Saul Alarcon", "2023"),
            ("Si no te pones las pilas...te va a cargar la chingada. -Saul Alarcon", "every day"),
            ("The key is... the people. -Saul Alarcon", "unknown date"),
            ("Nosotros tambien tenemos que tener sangre fria, porque si no, nos va a cargar la chingada. -Saul Alarcon", "7/14/2023"),
            ("Pues estoy bien pendejo y no te entiendo entonces. Será que estoy bien pendejo, o que? -Saul Alarcon", "8/8/2023"),
            ("Al buen entendedor, pocas palabras. -Saul Alarcon", "9/1/2023"),
            ("Nos sentimos como pensamos. Actuamos como pensamos. Y somos como pensamos. -Saul Alarcon", "9/1/2023"),
            ("Javier tiene el corazon de pollo, bien dentro, tiene el corazon de pollo. -Saul Alarcon", "9/4/2023"),
            ("Yo estoy aca, y soy el chilo de la pelicula, y aya ustedes son los monos o que? -Saul Alarcon", "9/4/2023"),
            ("No hay ningun rico aqui, fijense. Yo tambien fui jodido. Vengo de abajo! -Saul Alarcon", "9/8/2023"),
            ("A ver, vamos a analizar esto de una manera pizarronesca. -Saul Alarcon", "10/19/2023"),
            ("Yo, cuando veo una O mayuscula, una O de oportunidad, me la imagino de chocolate, mayuscula, asi grande, y ma la quiero comer YA! -Saul Alarcon", "11/6/2023"),
        ]

    def get_random_saulism(self) -> tuple[str, str]:
        return random.choice(self.sayings)

